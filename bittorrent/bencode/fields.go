package bencode

import (
	"reflect"
	"sort"
	"strings"
	"sync"
)

// field is one dictionary key of a struct: the path to its Go field
// through embedded structs, as reflect.Value.FieldByIndex takes it.
type field struct {
	name      string
	index     []int
	omitEmpty bool
}

// structInfo is a struct's keys, sorted as they are encoded.
type structInfo struct {
	fields []field
	byName map[string]int
}

var structCache sync.Map // reflect.Type -> *structInfo

func cachedStructInfo(t reflect.Type) *structInfo {
	if si, ok := structCache.Load(t); ok {
		return si.(*structInfo)
	}
	si := buildStructInfo(t)
	actual, _ := structCache.LoadOrStore(t, si)
	return actual.(*structInfo)
}

// buildStructInfo collects the fields of t and of the structs it embeds
// without a tag, with encoding/json's rule for names that clash: the
// shallowest field wins, and of several at that depth a tagged one; if
// that still leaves more than one, the name is dropped.
func buildStructInfo(t reflect.Type) *structInfo {
	type candidate struct {
		field
		depth  int
		tagged bool
	}
	var all []candidate
	var walk func(t reflect.Type, index []int, depth int, seen map[reflect.Type]bool)
	walk = func(t reflect.Type, index []int, depth int, seen map[reflect.Type]bool) {
		if seen[t] {
			return
		}
		seen[t] = true
		defer delete(seen, t)
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			tag, hasTag := sf.Tag.Lookup("bencode")
			if tag == "-" {
				continue
			}
			name, opts, _ := strings.Cut(tag, ",")
			idx := append(append([]int(nil), index...), i)
			ft := sf.Type
			if sf.Anonymous && name == "" {
				et := ft
				if et.Kind() == reflect.Pointer {
					et = et.Elem()
				}
				if et.Kind() == reflect.Struct {
					// An embedded pointer to an unexported struct cannot be
					// allocated through reflection, so it is skipped.
					if !sf.IsExported() && ft.Kind() == reflect.Pointer {
						continue
					}
					walk(et, idx, depth+1, seen)
					continue
				}
			}
			if !sf.IsExported() {
				continue
			}
			if name == "" {
				name = sf.Name
			}
			all = append(all, candidate{
				field:  field{name: name, index: idx, omitEmpty: hasOption(opts, "omitempty")},
				depth:  depth,
				tagged: hasTag && name != "",
			})
		}
	}
	walk(t, nil, 0, map[reflect.Type]bool{})

	sort.SliceStable(all, func(i, j int) bool {
		if all[i].name != all[j].name {
			return all[i].name < all[j].name
		}
		if all[i].depth != all[j].depth {
			return all[i].depth < all[j].depth
		}
		return all[i].tagged && !all[j].tagged
	})
	si := &structInfo{byName: map[string]int{}}
	for i := 0; i < len(all); {
		j := i + 1
		for j < len(all) && all[j].name == all[i].name {
			j++
		}
		dominant := true
		if j-i > 1 && all[i+1].depth == all[i].depth && all[i+1].tagged == all[i].tagged {
			dominant = false
		}
		if dominant {
			si.byName[all[i].name] = len(si.fields)
			si.fields = append(si.fields, all[i].field)
		}
		i = j
	}
	return si
}

func hasOption(opts, want string) bool {
	for opts != "" {
		var o string
		o, opts, _ = strings.Cut(opts, ",")
		if o == want {
			return true
		}
	}
	return false
}

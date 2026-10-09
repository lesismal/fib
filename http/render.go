//go:build linux || darwin || windows

package http

import (
	"encoding/xml"
	"fmt"
	"html/template"
	stdhttp "net/http"
	"path/filepath"
	"strings"
)

// The methods below answer a request in one call, as the same-named methods
// of gin.Context and fiber.Ctx do. Each sends nothing and returns an error
// when it cannot build the body, so that the handler can answer otherwise.

// String responds with status and the text format and args give, as
// text/plain.
func (c *Context) String(status int, format string, args ...any) error {
	return c.respondEncoded(status, "text/plain; charset=utf-8", func(dst []byte) ([]byte, error) {
		if len(args) == 0 {
			return append(dst, format...), nil
		}
		return fmt.Appendf(dst, format, args...), nil
	})
}

// Data responds with status and data as it is, of contentType.
func (c *Context) Data(status int, contentType string, data []byte) error {
	return c.Respond(status, contentType, data)
}

// IndentedJSON is JSON with the value indented by four spaces. It does not
// use JSONEncoder.
func (c *Context) IndentedJSON(status int, v any) error {
	return c.respondEncoded(status, "application/json", func(dst []byte) ([]byte, error) {
		return appendJSON(dst, v, indentOptions)
	})
}

// XML responds with status and v encoded as XML, as application/xml.
func (c *Context) XML(status int, v any) error {
	body, err := xml.Marshal(v)
	if err != nil {
		return err
	}
	return c.Respond(status, "application/xml; charset=utf-8", body)
}

// IndentedXML is XML with the value indented by four spaces.
func (c *Context) IndentedXML(status int, v any) error {
	body, err := xml.MarshalIndent(v, "", "    ")
	if err != nil {
		return err
	}
	return c.Respond(status, "application/xml; charset=utf-8", body)
}

// HTML responds with status and html as text/html.
func (c *Context) HTML(status int, html string) error {
	return c.respondEncoded(status, "text/html; charset=utf-8", func(dst []byte) ([]byte, error) {
		return append(dst, html...), nil
	})
}

// HTMLTemplate responds with status and the template name of tmpl executed
// with data, as text/html. An empty name executes tmpl itself.
func (c *Context) HTMLTemplate(status int, tmpl *template.Template, name string, data any) error {
	return c.respondEncoded(status, "text/html; charset=utf-8", func(dst []byte) ([]byte, error) {
		w := appendWriters.Get().(*appendWriter)
		w.out = dst
		var err error
		if name == "" {
			err = tmpl.Execute(w, data)
		} else {
			err = tmpl.ExecuteTemplate(w, name, data)
		}
		out := w.out
		w.out = nil
		appendWriters.Put(w)
		if err != nil {
			return dst, err
		}
		return out, nil
	})
}

// File responds with the file at filepath, with the Content-Type, ranges and
// conditional handling net/http.ServeFile gives. A missing file is a 404. The
// path is the program's own, not a request's: a request's must be cleaned
// first, as by FileCache.ServeFile.
func (c *Context) File(filepath string) {
	stdhttp.ServeFile(c, c.Request, filepath)
}

// FileAttachment is File with a Content-Disposition that has the client save
// the file as filename.
func (c *Context) FileAttachment(filepath, filename string) {
	c.Header().Set("Content-Disposition", `attachment; filename="`+attachmentName(filename)+`"`)
	stdhttp.ServeFile(c, c.Request, filepath)
}

// attachmentName is name as a quoted-string's content, with quotes, backslashes
// and control characters made harmless, and a directory part dropped.
func attachmentName(name string) string {
	name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	return strings.Map(func(r rune) rune {
		if r < ' ' || r == 0x7f || r == '"' {
			return '_'
		}
		return r
	}, name)
}

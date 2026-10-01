# Router benchmark

`fibhttp.Router` against [chi](https://github.com/go-chi/chi) v5 and net/http's `ServeMux`, on
the 203 routes of GitHub's v3 API (as [go-http-routing-benchmark](https://github.com/julienschmidt/go-http-routing-benchmark)
has them). Only routing is measured: each handler reads one parameter and writes nothing. fib's
requests are served on one recycled `Context`, as a server with `Config.ReuseContexts` gives
them; chi and `ServeMux` get a no-op `http.ResponseWriter`.

It is a module of its own so that fib does not depend on chi.

```sh
cd http/routerbench
go test -bench . -benchmem
```

## Results

Apple M4 Pro, Go 1.27.1, darwin/arm64, ns/op (allocs/op):

| Request | fib | chi | ServeMux |
| --- | ---: | ---: | ---: |
| Static `/user/repos` | 20.6 (0) | 115 (2) | 66.2 (0) |
| 1 param `/users/{user}` | 28.0 (0) | 198 (4) | 80.3 (1) |
| 2 params `/repos/{owner}/{repo}/branches` | 40.9 (0) | 240 (4) | 139 (2) |
| 3 params `/repos/{owner}/{repo}/issues/{number}/comments` | 48.7 (0) | 291 (4) | 194 (3) |
| 4 params `/legacy/issues/search/{owner}/{repository}/{state}/{keyword}` | 46.7 (0) | 291 (4) | 212 (3) |
| Not found | 13.6 (0) | 90.4 (2) | 509 (18) |
| All 203 routes | 9,500 (0) | 55,700 (740) | 32,100 (337) |

On a `Context` that is not recycled, a request with parameters allocates once more, for the
route state the `Context` then keeps.

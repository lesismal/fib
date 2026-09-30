module github.com/lesismal/fib/taskpool/benchmark

go 1.27

require (
	github.com/bytedance/gopkg v0.1.4
	github.com/lesismal/fib v0.0.0
	github.com/lesismal/nbio v1.7.0
	github.com/linfeip/fnet v0.0.0-20260927110723-b3e61d2437aa
	github.com/panjf2000/ants/v2 v2.12.1
)

require (
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/lesismal/fib => ../..

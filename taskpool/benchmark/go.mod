module github.com/lesismal/fib/taskpool/benchmark

go 1.27

require (
	github.com/bytedance/gopkg v0.1.4
	github.com/lesismal/fib v0.0.0
	github.com/lesismal/nbio v1.7.0
	github.com/panjf2000/ants/v2 v2.12.1
)

require golang.org/x/sync v0.11.0 // indirect

replace github.com/lesismal/fib => ../..

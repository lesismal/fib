// The ffmpeg interop checks of the media packages. They are a module of
// their own so that they can run ffmpeg and assume a Linux container
// without github.com/lesismal/fib's own tests needing either.
module github.com/lesismal/fib/media/interop

go 1.27.3

require github.com/lesismal/fib v0.0.0

replace github.com/lesismal/fib => ../..

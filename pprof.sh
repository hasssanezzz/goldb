curl http://localhost:6060/debug/pprof/profile?seconds=$1 > cpu.pprof
go tool pprof -http=:8080 ./main cpu.pprof

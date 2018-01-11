# tikv-shell
A command-line shell interface for tikv


```
install:

go get -u github.com/pingcap/tidb
cd $GOPATH/src/github.com/pingcap/tidb; make parser; cd -
go install github.com/c4pt0r/tikv-shell

usage:

./tikv-shell -pd [pd address of tikv cluster, default: localhost:2379]

commands:

put [key] [val]
puts [key1] [value1] [key2] [value2] ... [key N] [value N]
get [key]
seek [begin key(of key prefix)] [limit]
del [key1] [key2] ... [key N]
```

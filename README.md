# tikv-shell
A command-line shell interface for tikv

```
usage:

./tikv-shell -pd [pd address of tikv cluster, default: localhost:2379]

commands:

put [key] [val]
puts [key1] [value1] [key2] [value2] ... [key N] [value N]
get [key]
seek [begin key(of key prefix)] [limit]
```

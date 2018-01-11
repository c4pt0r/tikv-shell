package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/chzyer/readline"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/terror"

	goctx "golang.org/x/net/context"
)

type KV struct {
	K, V []byte
}

func (kv KV) String() string {
	return fmt.Sprintf("%s => %s (%v)", kv.K, kv.V, kv.V)
}

var (
	store  kv.Storage
	pdAddr = flag.String("pd", "localhost:2379", "pd address:localhost:2379")
)

// Init initializes information.
func initStore() {
	driver := tikv.Driver{}
	var err error
	store, err = driver.Open(fmt.Sprintf("tikv://%s", *pdAddr))
	terror.MustNil(err)
}

// key1 val1 key2 val2 ...
func puts(args ...[]byte) error {
	tx, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i < len(args); i += 2 {
		key, val := args[i], args[i+1]
		err := tx.Set(key, val)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = tx.Commit(goctx.Background())
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func doPut(args [][]byte) error {
	if len(args) != 2 {
		return errors.New("put [key] [value]")
	}
	return puts(args[0], args[1])
}

func doPuts(args [][]byte) error {
	if len(args) == 0 || len(args)%2 != 0 {
		return errors.New("puts [key1] [value1] [key2] [value2] ... [key N] [value N]")
	}
	return puts(args...)
}

func doGet(args [][]byte) (KV, error) {
	if len(args) != 1 {
		return KV{}, errors.New("get [key]")
	}
	tx, err := store.Begin()
	if err != nil {
		return KV{}, errors.Trace(err)
	}
	v, err := tx.Get(args[0])
	if err != nil {
		return KV{}, errors.Trace(err)
	}
	return KV{K: args[0], V: v}, nil
}

func doDel(args [][]byte) error {
	if len(args) == 0 {
		return errors.New("del [key 1] ... [key N]")
	}

	tx, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i < len(args); i++ {
		key := args[i]
		err := tx.Delete(key)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = tx.Commit(goctx.Background())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func doSeek(args [][]byte) ([]KV, error) {
	if len(args) != 2 {
		return nil, errors.New("seek [start key] [limit]")
	}

	tx, err := store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}

	keyPrefix := args[0]
	it, err := tx.Seek(keyPrefix)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cnt, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, errors.Trace(err)
	}
	var ret []KV
	for it.Valid() && cnt > 0 {
		ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		cnt--
		it.Next()
	}
	it.Close()
	return ret, nil
}

func do(cmd string, param [][]byte) (interface{}, error) {
	var ret interface{}
	var err error
	switch cmd {
	case "put":
		err = doPut(param)
	case "puts":
		err = doPuts(param)
	case "del":
		err = doDel(param)
	case "get":
		ret, err = doGet(param)
	case "seek":
		ret, err = doSeek(param)
	default:
		return nil, errors.New("usage: put | puts | get | seek | del")
	}
	return ret, err
}

func loop() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:            "tikv> ",
		HistoryFile:       "/tmp/readline.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}
		if line == "exit" {
			os.Exit(0)
		}

		if len(line) == 0 {
			continue
		}

		fields := bytes.Fields([]byte(line))
		cmd := strings.ToLower(string(fields[0]))
		parameters := fields[1:]

		if ret, err := do(cmd, parameters); err != nil {
			fmt.Println(err)
		} else {
			switch ret.(type) {
			case KV:
				fmt.Println(ret)
			case []KV:
				for _, kv := range ret.([]KV) {
					fmt.Println(kv)
				}
			case nil:
				fmt.Println("OK")
			}
		}
	}
}

func main() {
	pdAddr := os.Getenv("PD_ADDR")
	if pdAddr != "" {
		os.Args = append(os.Args, "-pd", pdAddr)
	}
	flag.Parse()
	initStore()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		switch sig {
		case syscall.SIGTERM:
			os.Exit(0)
		default:
			os.Exit(1)
		}
	}()
	loop()
}

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"dht/node"
)

func main() {
	port := flag.Int("port", 20000, "port to listen on")
	join := flag.String("join", "", "address of existing node to join")
	addr := flag.String("addr", "127.0.0.1", "address to advertise to other nodes")
	flag.Parse()

	node.SetLocalAddress(*addr)
	n := node.NewNode(*port)

	var wg sync.WaitGroup
	wg.Add(1)
	go n.Run(&wg)
	wg.Wait()

	if *join != "" {
		for i := 0; i < 30; i++ {
			if n.Join(*join) {
				break
			}
			time.Sleep(time.Second)
		}
	} else {
		n.Create()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	scanner := bufio.NewScanner(os.Stdin)
	cmdCh := make(chan string)

	go func() {
		for scanner.Scan() {
			cmdCh <- scanner.Text()
		}
		close(cmdCh)
	}()

	for {
		select {
		case <-sig:
			fmt.Println("received interrupt")
			n.Quit()
			return
		case line, ok := <-cmdCh:
			if !ok {
				n.Quit()
				return
			}
			parts := strings.Fields(line)
			if len(parts) == 0 {
				continue
			}
			switch parts[0] {
			case "put":
				if len(parts) < 3 {
					fmt.Println("usage: put <key> <value>")
					continue
				}
				ok := n.Put(parts[1], parts[2])
				if ok {
					fmt.Println("true")
				} else {
					fmt.Println("false")
				}
			case "get":
				if len(parts) < 2 {
					fmt.Println("usage: get <key>")
					continue
				}
				ok, value := n.Get(parts[1])
				if ok {
					fmt.Println(value)
				} else {
					fmt.Println("false")
				}
			case "delete":
				if len(parts) < 2 {
					fmt.Println("usage: delete <key>")
					continue
				}
				ok := n.Delete(parts[1])
				if ok {
					fmt.Println("true")
				} else {
					fmt.Println("false")
				}
			case "quit":
				n.Quit()
				return
			default:
				fmt.Printf("unknown command: %s\n", parts[0])
			}
		}
	}
}

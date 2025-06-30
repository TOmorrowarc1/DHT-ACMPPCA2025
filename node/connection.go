package node

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	MaxConnsPerHost = 5
)

type ConnectionPool struct {
	Conns map[string]chan *rpc.Client
	Lock  sync.Mutex
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		Conns: make(map[string]chan *rpc.Client),
	}
}

func (p *ConnectionPool) Dial(addr string) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

func (pool *ConnectionPool) Get(addr string) (*rpc.Client, error) {
	pool.Lock.Lock()
	channel, ok := pool.Conns[addr]
	if !ok {
		channel = make(chan *rpc.Client, MaxConnsPerHost)
		pool.Conns[addr] = channel
	}
	pool.Lock.Unlock()
	select {
	case client := <-channel:
		return client, nil
	default:
		return pool.Dial(addr)
	}
}

func (pool *ConnectionPool) Put(addr string, client *rpc.Client) {
	if client == nil {
		return
	}
	pool.Lock.Lock()
	channel, ok := pool.Conns[addr]
	pool.Lock.Unlock()
	if !ok {
		client.Close()
		return
	}
	select {
	case channel <- client:
	default:
		client.Close()
	}
}

func (pool *ConnectionPool) Close() {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	for addr, channel := range pool.Conns {
		close(channel)
		for client := range channel {
			client.Close()
		}
		delete(pool.Conns, addr)
	}
}
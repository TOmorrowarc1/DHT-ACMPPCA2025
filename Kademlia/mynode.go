package node

import (
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	HashBits  = 64
	KValue    = 8
	Alpha     = 3
	Exp       = 3
	TombStone = "TombStone"
)

// Note: The init() function will be executed when this package is imported.
// See https://golang.org/doc/effective_go.html#init for more details.
func init() {
	// You can use the logrus package to print pretty logs.
	// Here we set the log output to a file.
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

func XORDistance(lhs string, rhs string) uint64 {
	return FNV1aHash(lhs) ^ FNV1aHash(rhs)
}

func FNV1aHash(key string) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	length := len(key)
	for i := 0; i < length; i++ {
		hash ^= uint64(key[i])
		hash *= 0x100000001b3
	}
	return hash
}

func Uint64n(min uint64, max uint64) uint64 {
	length := max - min + 1
	return min + rand.Uint64()%length
}

type Content struct {
	Value     string
	TimeStamp int64
	Hot       int
}

type Node struct {
	Route    RouteList
	Data     map[uint64]Content
	DataLock sync.RWMutex

	listener       net.Listener
	server         *rpc.Server
	pool           *ConnectionPool
	Online         uint32
	BackGroundWait sync.WaitGroup
}

// RPC: NetWork Service.
func (node *Node) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.Route.SelfInfo())
	wg.Done()
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for atomic.LoadUint32(&node.Online) == 1 {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

func (node *Node) StopRPCServer() {
	node.listener.Close()
	node.pool.Close()
}

func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Node.RPCPing" && method != "Node.RPCFindClosestNodes" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.Route.SelfInfo(), addr, method, args)
	}
	client, err := node.pool.Get(addr)
	if err != nil {
		logrus.Error("failed to get connection from pool: ", err)
		return err
	}
	err = client.Call(method, args, reply)
	if err != nil {
		client.Close()
		logrus.Error("RemoteCall error: ", err)
		return err
	} else {
		node.pool.Put(addr, client)
		return nil
	}
}

// RPC Methods
type IDPair struct {
	Addr string
	ID   uint64
}

func (node *Node) RPCFindClosestNodes(pair IDPair, result *[]string) error {
	node.Route.Acknowledge(pair.Addr)
	*result = node.Route.FindClosestNodes(pair.ID)
	return nil
}

type ContentPair struct {
	Addr string
	ID   uint64
	Text Content
}

func (node *Node) RPCStore(pair ContentPair, _ *struct{}) error {
	node.Route.Acknowledge(pair.Addr)
	node.DataLock.Lock()
	defer node.DataLock.Unlock()
	if text_now, exist := node.Data[pair.ID]; !exist || text_now.TimeStamp <= pair.Text.TimeStamp {
		if pair.Text.Value != TombStone {
			pair.Text.Hot = 0
		}
		node.Data[pair.ID] = pair.Text
	}
	return nil
}

func (node *Node) RPCGet(key string, result *Content) error {
	node.DataLock.RLock()
	defer node.DataLock.RUnlock()
	if value, exists := node.Data[FNV1aHash(key)]; exists {
		*result = value
	}
	return nil
}

func (node *Node) RPCPing(addr string, reply *bool) error {
	node.Route.Acknowledge(addr)
	*reply = true
	return nil
}

func (node *Node) RPCEraseAddr(addr string, _ *struct{}) error {
	node.Route.EraseAddress(addr)
	return nil
}

// Routing algorithm
type CandidateNodes struct {
	Addr     string
	Distance uint64
}

func (node *Node) FindNodesID(target_id uint64) []string {
	has_quired := make(map[string]struct{})
	failed_nodes := make(map[string]struct{})
	shortlist := make([]CandidateNodes, 0)
	start := node.Route.FindClosestNodes(target_id)
	for _, possible := range start {
		if possible == "" {
			break
		}
		new_node := CandidateNodes{
			Addr:     possible,
			Distance: target_id ^ FNV1aHash(possible),
		}
		shortlist = append(shortlist, new_node)
	}
	sort.Slice(shortlist, func(i, j int) bool {
		return shortlist[i].Distance < shortlist[j].Distance
	})
	for {
		var quiry_nodes []string
		length := len(shortlist)
		for cursor := 0; cursor < length && len(quiry_nodes) < Alpha; cursor++ {
			if _, exists := has_quired[shortlist[cursor].Addr]; !exists {
				quiry_nodes = append(quiry_nodes, shortlist[cursor].Addr)
			}
		}
		if len(quiry_nodes) == 0 {
			break
		}
		resultsChan := make(chan []string, len(quiry_nodes))
		failedChan := make(chan string, len(quiry_nodes))
		var wait sync.WaitGroup
		for _, candidate := range quiry_nodes {
			has_quired[candidate] = struct{}{}
			wait.Add(1)
			go func(quiry_node string) {
				defer wait.Done()
				var reply []string
				target := IDPair{
					Addr: node.Route.SelfInfo(),
					ID:   target_id,
				}
				err := node.RemoteCall(quiry_node, "Node.RPCFindClosestNodes", target, &reply)
				if err == nil {
					resultsChan <- reply
					node.Route.Acknowledge(quiry_node)
				} else {
					failedChan <- quiry_node
					node.Route.EraseAddress(quiry_node)
				}
			}(candidate)
		}
		wait.Wait()
		close(resultsChan)
		close(failedChan)
		for failed_node := range failedChan {
			failed_nodes[failed_node] = struct{}{}
			length := len(shortlist)
			for cursor := 0; cursor < length; cursor++ {
				if shortlist[cursor].Addr == failed_node {
					shortlist = append(shortlist[:cursor], shortlist[cursor+1:]...)
					break
				}
			}
		}
		for new_nodes := range resultsChan {
			for _, possible := range new_nodes {
				is_new := true
				length := len(shortlist)
				for cursor := 0; cursor < length; cursor++ {
					if possible == shortlist[cursor].Addr {
						is_new = false
						break
					}
				}
				_, failed := failed_nodes[possible]
				if is_new && !failed {
					new_node := CandidateNodes{
						Addr:     possible,
						Distance: target_id ^ FNV1aHash(possible),
					}
					shortlist = append(shortlist, new_node)
				}
			}
			sort.Slice(shortlist, func(i, j int) bool {
				return shortlist[i].Distance < shortlist[j].Distance
			})
			if len(shortlist) > KValue {
				shortlist = shortlist[:KValue]
			}
		}
	}
	var answer []string
	for _, result := range shortlist {
		answer = append(answer, result.Addr)
	}
	if len(answer) == 0 {
		answer = append(answer, node.Route.SelfInfo())
	}
	return answer
}
func (node *Node) FindNodes(addr string) []string {
	return node.FindNodesID(FNV1aHash(addr))
}

func (node *Node) Ping(addr string) bool {
	flag := false
	err := node.RemoteCall(addr, "Node.RPCPing", "", &flag)
	if err == nil {
		return flag
	}
	return false
}

// Background threads keeping the structure and data.
func (node *Node) Publish(hot_limit int) {
	var publish_key []uint64
	var publish_value []Content
	var delete_key []uint64
	node.DataLock.Lock()
	for key, value := range node.Data {
		if value.Hot > hot_limit {
			publish_key = append(publish_key, key)
			publish_value = append(publish_value, value)
			if value.Hot > Exp {
				delete_key = append(delete_key, key)
			}
		}
		value.Hot++
		node.Data[key] = value
	}
	for _, key := range delete_key {
		delete(node.Data, key)
	}
	node.DataLock.Unlock()
	length := len(publish_key)
	for cursor := 0; cursor < length; cursor++ {
		target_nodes := node.FindNodesID(publish_key[cursor])
		for _, target_node := range target_nodes {
			info := ContentPair{
				Addr: node.Route.SelfInfo(),
				ID:   publish_key[cursor],
				Text: publish_value[cursor],
			}
			node.RemoteCall(target_node, "Node.RPCStore", info, nil)
		}
	}
}

func (node *Node) Refresh() {
	target_id := node.Route.RefreshID()
	if target_id != 0 {
		node.FindNodesID(target_id)
	}
}

func (node *Node) BackGroundStart() {
	node.BackGroundWait.Add(2)
	go func() {
		defer node.BackGroundWait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.Refresh()
			time.Sleep(500 * time.Millisecond)
		}
	}()
	go func() {
		defer node.BackGroundWait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.Publish(0)
			time.Sleep(500 * time.Millisecond)
		}
	}()
}

// DHT Methods
func (node *Node) Init(addr string) {
	node.Route.Init(addr)
	node.pool = NewConnectionPool()
	node.DataLock.Lock()
	defer node.DataLock.Unlock()
	node.Data = make(map[uint64]Content)
}

func (node *Node) Run(wg *sync.WaitGroup) {
	atomic.StoreUint32(&node.Online, 1)
	node.RunRPCServer(wg)
}

func (node *Node) Create() {
	node.BackGroundStart()
}

func (node *Node) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	node.Route.Acknowledge(addr)
	node.FindNodes(addr)
	node.BackGroundStart()
	return true
}

func (node *Node) Quit() {
	if atomic.LoadUint32(&node.Online) == 0 {
		return
	}
	//Hand all the items out, no matter it has published or not.
	node.Publish(-1)
	self_addr := node.Route.SelfInfo()
	target_nodes := node.Route.ReturnAll()
	for _, target_node := range target_nodes {
		node.RemoteCall(target_node, "Node.RPCEraseAddr", self_addr, nil)
	}
	atomic.StoreUint32(&node.Online, 0)
	node.StopRPCServer()
	node.BackGroundWait.Wait()
}

func (node *Node) ForceQuit() {
	atomic.StoreUint32(&node.Online, 0)
	node.StopRPCServer()
	node.BackGroundWait.Wait()
}

func (node *Node) Put(key string, value string) bool {
	target_nodes := node.FindNodes(key)
	pair := ContentPair{
		Addr: node.Route.SelfInfo(),
		ID:   FNV1aHash(key),
		Text: Content{
			Value:     value,
			TimeStamp: time.Now().UnixNano(),
		},
	}
	for _, target_node := range target_nodes {
		node.RemoteCall(target_node, "Node.RPCStore", pair, nil)
	}
	return true
}

func (node *Node) Get(key string) (bool, string) {
	target_nodes := node.FindNodes(key)
	var result Content
	var value Content
	for _, target_node := range target_nodes {
		node.RemoteCall(target_node, "Node.RPCGet", key, &value)
		if value.Value != "" && value.TimeStamp > result.TimeStamp {
			result = value
		}
	}
	if result.Value == "" || result.Value == TombStone {
		return false, result.Value
	}
	return true, result.Value
}

func (node *Node) Delete(key string) bool {
	exist, value := node.Get(key)
	if !exist || value == TombStone {
		return false
	}
	return node.Put(key, TombStone)
}

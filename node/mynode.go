// This package implements the chord DHT protocol.
package node

import (
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Note: The init() function will be executed when this package is imported.
// See https://golang.org/doc/effective_go.html#init for more details.
func init() {
	// You can use the logrus package to print pretty logs.
	// Here we set the log output to a file.
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

const RingSize uint64 = 1<<64 - 1

type Node struct {
	Addr   string // address and port number of the node, e.g., "localhost:1234"
	online bool

	Listener      net.Listener
	Server        *rpc.Server
	Predecessor   string
	SuccessorList []string
	NodeInfoLock  sync.RWMutex
	FingersTable  []string
	TableLock     sync.RWMutex
	Data          map[uint64]map[uint64]string
	DataLock      sync.RWMutex
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.

type DataPair struct {
	Node  uint64
	Key   uint64
	Value string
}

type StringBoolPair struct {
	value string
	ok    bool
}

type MapPair struct {
	Node uint64
	Map  map[uint64]string
}

type NodeInfo struct {
	Addr          string
	Predecessor   string
	SuccessorList []string
}

// Hash: from string to uint64
func (node *Node) FNV1aHash(key string) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= 0x100000001b3
	}
	return hash
}

func (node *Node) IsBetween(start uint64, end uint64, target uint64) bool {
	if start > end {
		return (target <= start || target > end)
	}
	return (target > start && target <= end)
}

func (node *Node) SafeGet(pair DataPair) (string, bool) {
	node.DataLock.RLock()
	defer node.DataLock.RUnlock()
	if inner, ok := node.Data[pair.Node]; ok {
		value, exists := inner[pair.Key]
		return value, exists // 返回inner的查找结果 (value, bool)
	}
	return "", false
}

func (node *Node) SafeWrite(pair DataPair) {
	node.DataLock.Lock()
	if node.Data[pair.Node] == nil {
		node.Data[pair.Node] = make(map[uint64]string)
	}
	node.Data[pair.Node][pair.Key] = pair.Value
	node.DataLock.Unlock()
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.Addr = addr
	node.Data = make(map[uint64]map[uint64]string)
	node.SuccessorList = make([]string, 0, 6)
	node.FingersTable = make([]string, 64)
}

func (node *Node) RunRPCServer() {
	node.Server = rpc.NewServer()
	node.Server.Register(node)
	var err error
	node.Listener, err = net.Listen("tcp", node.Addr)
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for node.online {
		conn, err := node.Listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.Server.ServeConn(conn)
	}
}

func (node *Node) StopRPCServer() {
	node.online = false
	node.Listener.Close()
}

// RemoteCall calls the RPC method at addr.
//
// Note: An empty interface can hold values of any type. (https://tour.golang.org/methods/14)
// Re-connect to the client every time can be slow. You can use connection pool to improve the performance.
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Node.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr, addr, method, args)
	}
	// Note: Here we use DialTimeout to set a timeout of 20 milliseconds.
	conn, err := net.DialTimeout("tcp", addr, 20*time.Millisecond)
	if err != nil {
		logrus.Error("dialing: ", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("RemoteCall error: ", err)
		return err
	}
	return nil
}

//
// RPC Methods
//

// Note: The methods used for RPC must be exported (i.e., Capitalized),
// and must have two arguments, both exported (or builtin) types.
// The second argument must be a pointer.
// The return type must be error.
// In short, the signature of the method must be:
//   func (t *T) MethodName(argType T1, replyType *T2) error
// See https://golang.org/pkg/net/rpc/ for more details.

// Here we use "_" to ignore the arguments we don't need.
// The empty struct "{}" is used to represent "void" in Go.
// These are functions in which the node serves as a server.
func (node *Node) FindClosestPredecessor(target_id uint64, reply *string) error {
	flag := false
	for i := len(node.FingersTable); i >= 0 && !flag; i-- {
		if !node.IsBetween(node.FNV1aHash(node.Addr), node.FNV1aHash(node.FingersTable[i]), target_id) {
			*reply = node.FingersTable[i]
			//Check the predecessor we find is online.
			node.RemoteCall(*reply, "Node.Pong", "", &flag)
		}
	}
	if !flag {
		*reply = node.Addr
	}
	return nil
}

func (node *Node) GetPair(key DataPair, reply *StringBoolPair) error {
	reply.value, reply.ok = node.SafeGet(key)
	return nil
}

func (node *Node) PutPair(pair DataPair, _ *struct{}) error {
	node.SafeWrite(pair)
	return nil
}

func (node *Node) PutData(data MapPair, _ *struct{}) error {
	node.DataLock.Lock()
	node.Data[data.Node] = data.Map
	node.DataLock.Unlock()
	return nil
}

func (node *Node) DeletePair(pair DataPair, flag *bool) error {
	_, ok := node.SafeGet(pair)
	if ok {
		delete(node.Data[pair.Node], pair.Key)
	}
	*flag = ok
	return nil
}

func (node *Node) DeleteData(pair DataPair, _ *struct{}) error {
	node.DataLock.Lock()
	delete(node.Data, pair.Node)
	node.DataLock.Unlock()
	return nil
}

func (node *Node) GetNodeInfo(_ string, reply *NodeInfo) error {
	node.NodeInfoLock.RLock()
	reply.Addr = node.Addr
	reply.Predecessor = node.Predecessor
	reply.SuccessorList = node.SuccessorList
	node.NodeInfoLock.RUnlock()
	return nil
}

func (node *Node) ChangeNodeInfo(new_info NodeInfo, _ *struct{}) error {
	node.NodeInfoLock.Lock()
	if new_info.Predecessor != "" {
		node.Predecessor = new_info.Predecessor
	}
	if new_info.SuccessorList[0] != "" {
		node.SuccessorList = new_info.SuccessorList
	}
	node.NodeInfoLock.Unlock()
	return nil
}

func (node *Node) Notify(predecessor string, _ *struct{}) error {
	node.NodeInfoLock.Lock()
	if node.Predecessor == "" || node.IsBetween(node.FNV1aHash(node.Predecessor), node.FNV1aHash(node.Addr), node.FNV1aHash(predecessor)) {
		node.Predecessor = predecessor
	}
	node.NodeInfoLock.Unlock()
	return nil
}

func (node *Node) Pong(_ string, flag *bool) error {
	*flag = true
	return nil
}

//
// DHT methods
//

func (node *Node) Run() {
	node.online = true
	go node.RunRPCServer()
}

func (node *Node) Create() {
	logrus.Info("Create")
}

func (node *Node) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	return true
}

// If the node misses the key, search for its position.
func (node *Node) FindPredecessor(target_id uint64) NodeInfo {
	node.NodeInfoLock.RLock()
	content := NodeInfo{
		Addr:        node.Addr,
		Predecessor: "nowhere",
	}
	node.NodeInfoLock.RUnlock()
	for content.Predecessor != content.Addr {
		content.Predecessor = content.Addr
		node.RemoteCall(content.Predecessor, "Node.FindClosestPredecessor", target_id, &content.Addr)
	}
	node.RemoteCall(content.Addr, "Node.GetNodeInfo", "", &content)
	return content
}

func (node *Node) Get(key string) (bool, string) {
	logrus.Infof("Get %s", key)
	target_id := node.FNV1aHash(key)
	node.NodeInfoLock.RLock()
	self_id := node.FNV1aHash(node.Addr)
	if node.IsBetween(node.FNV1aHash(node.Predecessor), self_id, target_id) {
		key := DataPair{self_id, target_id, ""}
		value, ok := node.SafeGet(key)
		return ok, value
	}
	node.NodeInfoLock.RUnlock()
	info := node.FindPredecessor(target_id)
	key_info := DataPair{
		Node: node.FNV1aHash(info.SuccessorList[0]),
		Key:  target_id,
	}
	var result StringBoolPair
	node.RemoteCall(info.SuccessorList[0], "Node.GetPair", key_info, &result)
	return result.ok, result.value
}

func (node *Node) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	target_id := node.FNV1aHash(key)
	flag := false
	node.NodeInfoLock.RLock()
	self_id := node.FNV1aHash(node.Addr)
	self_predecessor := node.FNV1aHash(node.Predecessor)
	if node.IsBetween(self_predecessor, self_id, target_id) {
		target := DataPair{self_id, target_id, value}
		node.SafeWrite(target)
		length := len(node.SuccessorList)
		for i := 0; i < 2 && i < length; i++ {
			node.RemoteCall(node.SuccessorList[i], "Node.PutPair", target, &flag)
		}
		return true
	}
	node.NodeInfoLock.RUnlock()
	info := node.FindPredecessor(target_id)
	key_info := DataPair{
		Node: node.FNV1aHash(info.SuccessorList[0]),
		Key:  target_id,
	}
	length := len(node.SuccessorList)
	for i := 0; i < 2 && i < length; i++ {
		cursor := i
		go func() {
			node.RemoteCall(node.SuccessorList[cursor], "Node.PutPair", key_info, &flag)
		}()
	}
	return true
}

func (node *Node) Delete(key string) bool {
	logrus.Infof("Delete %s", key)
	target_id := node.FNV1aHash(key)
	flag := false
	node.NodeInfoLock.RLock()
	self_id := node.FNV1aHash(node.Addr)
	if node.IsBetween(node.FNV1aHash(node.Predecessor), self_id, target_id) {
		target := DataPair{
			Node: self_id,
			Key:  target_id,
		}
		node.DataLock.Lock()
		delete(node.Data[target.Node], target.Key)
		node.DataLock.Unlock()
		length := len(node.SuccessorList)
		for i := 0; i < 2 && i < length; i++ {
			node.RemoteCall(node.SuccessorList[i], "Node.DeletePair", target, &flag)
		}
		return flag
	}
	node.NodeInfoLock.RUnlock()
	info := node.FindPredecessor(target_id)
	key_info := DataPair{node.FNV1aHash(info.SuccessorList[0]), target_id, ""}
	length := len(node.SuccessorList)
	for i := 0; i < 2 && i < length; i++ {
		cursor := i
		go func() {
			node.RemoteCall(node.SuccessorList[cursor], "Node.DeletePair", key_info, &flag)
		}()
	}
	return flag
}

func (node *Node) Quit() {
	logrus.Infof("Quit %s", node.Addr)
	node.StopRPCServer()
}

func (node *Node) ForceQuit() {
	logrus.Info("ForceQuit")
	node.StopRPCServer()
}

func (node *Node) Stablize() {
	node.NodeInfoLock.RLock()
	current_node := NodeInfo{
		Addr:          node.Addr,
		SuccessorList: node.SuccessorList,
	}
	node.NodeInfoLock.RUnlock()
	if len(current_node.SuccessorList) == 0 {
		//The node has not entered the net.
		return
	}
	current_successor := NodeInfo{node.SuccessorList[0], "", node.SuccessorList}
	err := node.RemoteCall(current_successor.Addr, "Node.GetNodeInfo", "", &current_successor)
	if err != nil {
		//The health check failed.
		return
	}
	if current_successor.Predecessor != "" && node.IsBetween(node.FNV1aHash(current_node.Addr), node.FNV1aHash(current_successor.Addr), node.FNV1aHash(current_successor.Predecessor)) {
		node.NodeInfoLock.Lock()
		node.SuccessorList[0] = current_successor.Predecessor
		node.NodeInfoLock.Unlock()
	}
	err = node.RemoteCall(current_node.SuccessorList[0], "Node.Notify", current_node.Addr, nil)
	if err != nil {
		//health check.
		return
	}
}

func (node *Node) FixFingers() {
	i := rand.Intn(64)
	target_id := (node.FNV1aHash(node.Addr) + 1<<i) % RingSize
	node.FingersTable[i] = node.FindPredecessor(target_id).SuccessorList[0]
}

func (node *Node) PingPredecessor() {
	flag := false
	if node.RemoteCall(node.Predecessor, "Node.Pong", "", &flag) != nil || !flag {
		node.Predecessor = ""
	}
}

func (node *Node) SuccessorListPing() {
	flag := false
	cursor := 0
	var current_node NodeInfo
	var current_successor NodeInfo
	//Fix the successorlist.
	node.NodeInfoLock.RLock()
	current_node.SuccessorList = node.SuccessorList
	node.NodeInfoLock.RUnlock()
	for ; cursor < len(current_node.SuccessorList) && !flag; cursor++ {
		node.RemoteCall(current_node.SuccessorList[cursor], "Node.Pong", "", &flag)
	}
	node.RemoteCall(current_node.SuccessorList[cursor], "Node.GetNodeInfo", "", &current_successor)
	node.NodeInfoLock.Lock()
	node.SuccessorList[0] = current_node.SuccessorList[cursor]
	for i := 1; i < 6; i++ {
		node.SuccessorList[i] = current_successor.SuccessorList[i-1]
	}
	current_node.SuccessorList = node.SuccessorList
	current_node.Addr = node.Addr
	node.NodeInfoLock.Unlock()
	//Push the copies.
	self_id := node.FNV1aHash(current_node.Addr)
	node.DataLock.RLock()
	self_data := MapPair{
		Node: self_id,
		Map:  node.Data[self_id],
	}
	node.DataLock.RUnlock()
	for _, successor := range current_node.SuccessorList {
		current_addr := successor
		go func() {
			node.RemoteCall(current_addr, "Node.PutData", self_data, &flag)
		}()
	}
}

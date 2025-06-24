// This package implements the chord DHT protocol.
package node

import (
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
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
const ListSize int = 6

type Node struct {
	Addr string // address and port number of the node, e.g., "localhost:1234"

	Listener      net.Listener
	Server        *rpc.Server
	Predecessor   string
	SuccessorList []string
	NodeInfoLock  sync.RWMutex
	FingersTable  []string
	TableLock     sync.RWMutex
	Data          map[uint64]map[uint64]string
	DataLock      sync.RWMutex

	Online uint32
	Wait   sync.WaitGroup
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
	Value string
	Ok    bool
}

type MapIntPair struct {
	Map  map[uint64]string
	Node uint64
}

type NodeInfo struct {
	Addr          string
	Predecessor   string
	SuccessorList []string
}

// Hash: from string to uint64
func (node *Node) FNV1aHash(key string) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	length := len(key)
	for i := 0; i < length; i++ {
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
		return value, exists
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
	node.Data[node.FNV1aHash(addr)] = make(map[uint64]string)
	node.SuccessorList = make([]string, ListSize)
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
	for node.Online == 1 {
		conn, err := node.Listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.Server.ServeConn(conn)
	}
}

func (node *Node) StopRPCServer() {
	node.Listener.Close()
}

// RemoteCall calls the RPC method at addr.
//
// Note: An empty interface can hold values of any type. (https://tour.golang.org/methods/14)
// Re-connect to the client every time can be slow. You can use connection pool to improve the performance.
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Node.Pong" && method != "Node.Notify" {
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
	node.NodeInfoLock.RLock()
	logrus.Infof("Start to findclosestPredecessor on %s", node.Addr)
	self_id := node.FNV1aHash(node.Addr)
	node.NodeInfoLock.RUnlock()
	flag := false
	node.TableLock.RLock()
	for i := len(node.FingersTable) - 1; i >= 0 && !flag; i-- {
		if !node.IsBetween(self_id, node.FNV1aHash(node.FingersTable[i]), target_id) {
			*reply = node.FingersTable[i]
			//Check the predecessor we find is online.
			node.RemoteCall(*reply, "Node.Pong", "", &flag)
		}
	}
	node.TableLock.RUnlock()
	if !flag {
		node.NodeInfoLock.RLock()
		*reply = node.Addr
		node.NodeInfoLock.RUnlock()
	}
	logrus.Infof("One Predecessor %s", *reply)
	return nil
}

func (node *Node) GetPair(key DataPair, reply *StringBoolPair) error {
	reply.Value, reply.Ok = node.SafeGet(key)
	return nil
}

func (node *Node) PutPair(pair DataPair, _ *struct{}) error {
	node.SafeWrite(pair)
	return nil
}

func (node *Node) PutData(data MapIntPair, _ *struct{}) error {
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

func (node *Node) SplitData(target_id uint64, pair *MapIntPair) error {
	node.NodeInfoLock.RLock()
	self_id := node.FNV1aHash(node.Addr)
	node.NodeInfoLock.RUnlock()
	node.DataLock.Lock()
	node.Data[pair.Node] = make(map[uint64]string)
	for key, value := range node.Data[self_id] {
		if node.IsBetween(target_id, self_id, key) {
			pair.Map[key] = value
			delete(node.Data[self_id], key)
		}
	}
	node.DataLock.Unlock()
	return nil
}

func (node *Node) MergeData(target_id uint64, node_info *NodeInfo) error {
	//Return the nodeinfo
	node.NodeInfoLock.RLock()
	self_id := node.FNV1aHash(node.Addr)
	node_info.Addr = node.Addr
	node_info.Predecessor = node.Predecessor
	node_info.SuccessorList = node.SuccessorList
	node.NodeInfoLock.RUnlock()
	//Merge the data between the target and self into self, which indicates the disappear of nodes.
	node.DataLock.Lock()
	for key, value := range node.Data {
		if node.IsBetween(target_id, self_id, key) {
			for key_in, value_in := range value {
				node.Data[self_id][key_in] = value_in
			}
			delete(node.Data, key)
		}
	}
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
	if len(new_info.SuccessorList) != 0 {
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
	*flag = (node.Online == 1)
	return nil
}

//
// DHT methods
//

func (node *Node) Run() {
	node.Online = 1
	go node.RunRPCServer()
}

func (node *Node) Create() {
	logrus.Info("Create")
	node.NodeInfoLock.Lock()
	node.Predecessor = node.Addr
	node.SuccessorList[0] = node.Addr
	node.NodeInfoLock.Unlock()
	node.TableLock.Lock()
	for i := 0; i < 64; i++ {
		node.FingersTable[i] = node.Addr
	}
	node.TableLock.Unlock()
	go func() {
		defer node.Wait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.Stablize()
			time.Sleep(500 * time.Millisecond)
		}
	}()
	go func() {
		defer node.Wait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.FixFingers()
			time.Sleep(10 * time.Second)
		}
	}()
	go func() {
		defer node.Wait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.PingPredecessor()
			time.Sleep(500 * time.Millisecond)
		}
	}()
	go func() {
		defer node.Wait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.PingSuccessorList()
			time.Sleep(10 * time.Second)
		}
	}()
}

func (node *Node) Join(addr string) bool {
	node_info := NodeInfo{
		Addr: addr,
	}
	node.NodeInfoLock.RLock()
	logrus.Infof("Join %s through %s", node.Addr, addr)
	target_id := node.FNV1aHash(node.Addr)
	node.NodeInfoLock.RUnlock()
	for node_info.Addr != node_info.Predecessor {
		node_info.Predecessor = node_info.Addr
		node.RemoteCall(node_info.Predecessor, "Node.FindClosestPredecessor", target_id, &node_info.Addr)
	}
	node.RemoteCall(node_info.Predecessor, "Node.GetNodeInfo", "", &node_info)
	node.NodeInfoLock.Lock()
	node.Predecessor = node_info.Addr
	node.SuccessorList = node_info.SuccessorList
	node.NodeInfoLock.Unlock()
	flag := false
	node.RemoteCall(node_info.SuccessorList[0], "Node.Notify", node.Addr, &flag)
	node_data := MapIntPair{
		Map:  make(map[uint64]string),
		Node: target_id,
	}
	node.RemoteCall(node_info.SuccessorList[0], "Node.SplitData", "", &node_data)
	node.DataLock.Lock()
	node.Data[node_data.Node] = node_data.Map
	node.DataLock.Unlock()
	node.Wait.Add(4)
	go func() {
		defer node.Wait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.Stablize()
			time.Sleep(500 * time.Millisecond)
		}
	}()
	go func() {
		defer node.Wait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.FixFingers()
			time.Sleep(10 * time.Second)
		}
	}()
	go func() {
		defer node.Wait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.PingPredecessor()
			time.Sleep(500 * time.Millisecond)
		}
	}()
	go func() {
		defer node.Wait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.PingSuccessorList()
			time.Sleep(10 * time.Second)
		}
	}()
	return true
}

func (node *Node) Quit() {
	logrus.Infof("Quit %s", node.Addr)
	node.NodeInfoLock.RLock()
	node_info_front := NodeInfo{
		Addr:          node.Predecessor,
		SuccessorList: node.SuccessorList,
	}
	node_info_next := NodeInfo{
		Addr:        node.SuccessorList[0],
		Predecessor: node.Predecessor,
	}
	node.NodeInfoLock.RUnlock()
	node.RemoteCall(node_info_front.Addr, "Node.ChangeNodeInfo", node_info_front, nil)
	node.RemoteCall(node_info_next.Addr, "Node.ChangeNodeInfo", node_info_next, nil)
	node.RemoteCall(node_info_next.Addr, "Node.MergeData", node.FNV1aHash(node_info_front.Addr), &node_info_front)
	atomic.StoreUint32(&node.Online, 0)
	node.StopRPCServer()
	node.Wait.Wait()
}

func (node *Node) ForceQuit() {
	logrus.Info("ForceQuit")
	atomic.StoreUint32(&node.Online, 0)
	node.StopRPCServer()
	node.Wait.Wait()
}

// If the node misses the key, search for its position.
func (node *Node) FindPredecessor(target_id uint64) NodeInfo {
	node.NodeInfoLock.RLock()
	content := NodeInfo{
		Addr: node.Addr,
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
	node.NodeInfoLock.RUnlock()
	if node.IsBetween(node.FNV1aHash(node.Predecessor), self_id, target_id) {
		key := DataPair{
			Node: self_id,
			Key:  target_id,
		}
		value, ok := node.SafeGet(key)
		return ok, value
	}
	info := node.FindPredecessor(target_id)
	key_info := DataPair{
		Node: node.FNV1aHash(info.SuccessorList[0]),
		Key:  target_id,
	}
	var result StringBoolPair
	node.RemoteCall(info.SuccessorList[0], "Node.GetPair", key_info, &result)
	return result.Ok, result.Value
}

func (node *Node) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	target_id := node.FNV1aHash(key)
	flag := false
	node.NodeInfoLock.RLock()
	self_id := node.FNV1aHash(node.Addr)
	self_predecessor := node.FNV1aHash(node.Predecessor)
	node.NodeInfoLock.RUnlock()
	if node.IsBetween(self_predecessor, self_id, target_id) {
		target := DataPair{
			self_id,
			target_id,
			value,
		}
		node.SafeWrite(target)
		for cursor := 0; cursor < 2 && node.SuccessorList[cursor] != ""; cursor++ {
			node.RemoteCall(node.SuccessorList[cursor], "Node.PutPair", target, &flag)
		}
		return true
	}
	info := node.FindPredecessor(target_id)
	key_info := DataPair{
		Node: node.FNV1aHash(info.SuccessorList[0]),
		Key:  target_id,
	}
	for cursor := 0; cursor < 2 && node.SuccessorList[cursor] != ""; cursor++ {
		current_cursor := cursor
		go func() {
			node.RemoteCall(node.SuccessorList[current_cursor], "Node.PutPair", key_info, &flag)
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
	node.NodeInfoLock.RUnlock()
	if node.IsBetween(node.FNV1aHash(node.Predecessor), self_id, target_id) {
		target := DataPair{
			Node: self_id,
			Key:  target_id,
		}
		node.DataLock.Lock()
		delete(node.Data[target.Node], target.Key)
		node.DataLock.Unlock()
		for cursor := 0; cursor < 2 && node.SuccessorList[cursor] != ""; cursor++ {
			node.RemoteCall(node.SuccessorList[cursor], "Node.DeletePair", target, &flag)
		}
		return flag
	}
	info := node.FindPredecessor(target_id)
	key_info := DataPair{
		Node: node.FNV1aHash(info.SuccessorList[0]),
		Key:  target_id,
	}
	for cursor := 0; cursor < 2 && node.SuccessorList[cursor] != ""; cursor++ {
		current_cursor := cursor
		go func() {
			node.RemoteCall(node.SuccessorList[current_cursor], "Node.DeletePair", key_info, &flag)
		}()
	}
	return flag
}

func (node *Node) Stablize() {
	node.NodeInfoLock.RLock()
	current_node := NodeInfo{
		Addr:          node.Addr,
		SuccessorList: node.SuccessorList,
	}
	node.NodeInfoLock.RUnlock()
	if current_node.SuccessorList[0] == "" {
		//The node has not entered the net.
		return
	}
	current_successor := NodeInfo{
		Addr: node.SuccessorList[0],
	}
	err := node.RemoteCall(current_successor.Addr, "Node.GetNodeInfo", "", &current_successor)
	if err != nil {
		//The health check failed.
		cursor := 1
		flag := false
		for ; current_node.SuccessorList[cursor] != "" && !flag; cursor++ {
			node.RemoteCall(current_node.SuccessorList[cursor], "Node.Pong", "", &flag)
		}
		cursor--
		node.RemoteCall(current_node.SuccessorList[cursor], "Node.MergeData", node.FNV1aHash(current_node.Addr), &current_successor)
		node.NodeInfoLock.Lock()
		node.SuccessorList[0] = current_successor.Addr
		for i := 1; i < 6; i++ {
			node.SuccessorList[i] = current_successor.SuccessorList[i-1]
		}
		current_node.SuccessorList = node.SuccessorList
		current_node.Addr = node.Addr
		node.NodeInfoLock.Unlock()
	}
	if current_successor.Predecessor != "" && node.IsBetween(node.FNV1aHash(current_node.Addr), node.FNV1aHash(current_successor.Addr), node.FNV1aHash(current_successor.Predecessor)) {
		node.NodeInfoLock.Lock()
		node.SuccessorList[0] = current_successor.Predecessor
		node.NodeInfoLock.Unlock()
	}
	node.RemoteCall(current_node.SuccessorList[0], "Node.Notify", current_node.Addr, nil)
}

func (node *Node) FixFingers() {
	var addr string
	node.NodeInfoLock.RLock()
	addr = node.SuccessorList[0]
	node.NodeInfoLock.RUnlock()
	if addr == "" {
		//The node has not entered the net.
		return
	}
	i := rand.Intn(64)
	target_id := (node.FNV1aHash(node.Addr) + 1<<i) % RingSize
	addr = node.FindPredecessor(target_id).SuccessorList[0]
	node.TableLock.Lock()
	node.FingersTable[i] = addr
	node.TableLock.Unlock()
}

func (node *Node) PingPredecessor() {
	flag := false
	if node.Predecessor == "" || node.RemoteCall(node.Predecessor, "Node.Pong", "", &flag) != nil || !flag {
		node.Predecessor = ""
	}
}

func (node *Node) PingSuccessorList() {
	flag := false
	cursor := 0
	var current_node NodeInfo
	var current_successor NodeInfo
	//Fix the successorlist.
	node.NodeInfoLock.RLock()
	current_node.SuccessorList = node.SuccessorList
	node.NodeInfoLock.RUnlock()
	for ; cursor < ListSize && current_node.SuccessorList[cursor] != "" && !flag; cursor++ {
		node.RemoteCall(current_node.SuccessorList[cursor], "Node.Pong", "", &flag)
	}
	cursor--
	if cursor != 0 && current_node.SuccessorList[cursor] != "" {
		node.RemoteCall(current_node.SuccessorList[cursor], "Node.MergeData", node.FNV1aHash(current_node.Addr), &current_successor)
		node.NodeInfoLock.Lock()
		node.SuccessorList[0] = current_node.SuccessorList[cursor]
		for i := 1; i < 6; i++ {
			node.SuccessorList[i] = current_successor.SuccessorList[i-1]
		}
		current_node.SuccessorList = node.SuccessorList
		current_node.Addr = node.Addr
		node.NodeInfoLock.Unlock()
	}
	//Push the copies.
	self_id := node.FNV1aHash(current_node.Addr)
	node.DataLock.RLock()
	self_data := MapIntPair{
		Node: self_id,
		Map:  node.Data[self_id],
	}
	node.DataLock.RUnlock()
	for cursor := 0; cursor < ListSize && current_node.SuccessorList[cursor] != ""; cursor++ {
		current_addr := current_node.SuccessorList[cursor]
		go func() {
			node.RemoteCall(current_addr, "Node.PutData", self_data, nil)
		}()
	}
}

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
	Addr          string
	Predecessor   string
	SuccessorList []string
	NodeInfoLock  sync.RWMutex

	FingersTable []string
	TableLock    sync.RWMutex

	Data     map[uint64]map[uint64]string
	DataLock sync.RWMutex

	Online   uint32
	Listener net.Listener
	Server   *rpc.Server
	Wait     sync.WaitGroup
}

// Some basic types serves as parameters and contents.
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
func FNV1aHash(key string) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	length := len(key)
	for i := 0; i < length; i++ {
		hash ^= uint64(key[i])
		hash *= 0x100000001b3
	}
	return hash
}

func IsBetween(start uint64, end uint64, target uint64) bool {
	if start >= end {
		return (target > start || target <= end)
	}
	return (target > start && target <= end)
}

/*Sealing: Data control*/
func (node *Node) GetPair(pair DataPair) (string, bool) {
	node.DataLock.RLock()
	defer node.DataLock.RUnlock()
	if inner, ok := node.Data[pair.Node]; ok {
		value, exists := inner[pair.Key]
		return value, exists
	}
	return "", false
}

func (node *Node) PutPair(pair DataPair) {
	node.DataLock.Lock()
	if node.Data[pair.Node] == nil {
		node.Data[pair.Node] = make(map[uint64]string)
	}
	node.Data[pair.Node][pair.Key] = pair.Value
	node.DataLock.Unlock()
}

func (node *Node) DeletePair(pair DataPair) (string, bool) {
	node.DataLock.Lock()
	var value string
	var exists bool
	if inner, ok := node.Data[pair.Node]; ok {
		value, exists = inner[pair.Key]
		delete(inner, pair.Key)
	}
	node.DataLock.Unlock()
	return value, exists
}

func (node *Node) PutCopy(pair MapIntPair) {
	node.DataLock.Lock()
	node.Data[pair.Node] = pair.Map
	node.DataLock.Unlock()
}

func (node *Node) DeleteCopy(id uint64) bool {
	ok := false
	if _, ok = node.Data[id]; ok {
		delete(node.Data, id)
	}
	return ok
}

func (node *Node) SplitCopy(end_id uint64, target_id uint64) map[uint64]string {
	var result map[uint64]string = make(map[uint64]string)
	for key, value := range node.Data[target_id] {
		if !IsBetween(end_id, target_id, key) {
			result[key] = value
			delete(node.Data[target_id], key)
		}
	}
	return result
}

func (node *Node) MergeCopy(end_id uint64, target_id uint64) {
	node.DataLock.Lock()
	for key, value := range node.Data {
		if !IsBetween(end_id, target_id, key) && key != target_id {
			for key_in, value_in := range value {
				node.Data[target_id][key_in] = value_in
			}
			delete(node.Data, key)
		}
	}
	logrus.Infof("The copy %d after merge is %v", target_id, node.Data[target_id])
	node.DataLock.Unlock()
}

// Sealing: RPC services and RPC methods.
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
	node.NodeInfoLock.RLock()
	addr := node.Addr
	node.NodeInfoLock.RUnlock()
	node.Listener.Close()
	logrus.Infof("RPC listener on %s closed", addr)
}

// Re-connect to the client every time can be slow. You can use connection pool to improve the performance.
func (node *Node) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Node.Pong" && method != "Node.Notify" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr, addr, method, args)
	}
	// Note: Here we use DialTimeout to set a timeout of 20 milliseconds.
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
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

// RPC methods for topological structures.
func (node *Node) GetNodeInfo(_ string, reply *NodeInfo) error {
	node.NodeInfoLock.RLock()
	reply.Addr = node.Addr
	reply.Predecessor = node.Predecessor
	reply.SuccessorList = make([]string, ListSize)
	copy(reply.SuccessorList, node.SuccessorList)
	logrus.Infof("NodeInfo: %s %s %v", node.Addr, node.Predecessor, node.SuccessorList)
	node.NodeInfoLock.RUnlock()
	return nil
}

func (node *Node) ChangeNodeInfo(new_info NodeInfo, _ *struct{}) error {
	node.NodeInfoLock.Lock()
	if new_info.Predecessor != "" {
		node.Predecessor = new_info.Predecessor
	}
	if len(new_info.SuccessorList) != 0 && new_info.SuccessorList[0] != "" {
		copy(node.SuccessorList, new_info.SuccessorList)
	}
	node.NodeInfoLock.Unlock()
	return nil
}

func (node *Node) Notify(predecessor string, _ *struct{}) error {
	node.NodeInfoLock.Lock()
	if node.Predecessor == "" || IsBetween(FNV1aHash(node.Predecessor), FNV1aHash(node.Addr), FNV1aHash(predecessor)) {
		node.Predecessor = predecessor
	}
	node.NodeInfoLock.Unlock()
	return nil
}

func (node *Node) Pong(_ string, flag *bool) error {
	*flag = (node.Online == 1)
	return nil
}

func (node *Node) FindClosestPredecessor(target_id uint64, reply *string) error {
	node.NodeInfoLock.RLock()
	self_id := FNV1aHash(node.Addr)
	node.NodeInfoLock.RUnlock()
	flag := false
	for i := len(node.FingersTable) - 1; i >= 0 && !flag; i-- {
		node.TableLock.RLock()
		finger := node.FingersTable[i]
		node.TableLock.RUnlock()
		if finger != "" && IsBetween(self_id, target_id, FNV1aHash(finger)) && FNV1aHash(finger) != target_id {
			*reply = finger
			//Check the predecessor we find is online.
			node.RemoteCall(finger, "Node.Pong", "", &flag)
		}
	}
	if !flag {
		node.NodeInfoLock.RLock()
		if IsBetween(self_id, target_id, FNV1aHash(node.SuccessorList[0])) {
			*reply = node.SuccessorList[0]
		} else {
			*reply = node.Addr
		}
		node.NodeInfoLock.RUnlock()
	}
	logrus.Infof("Predecessor on %d for %d is %s", self_id, target_id, *reply)
	return nil
}

func (node *Node) RPCFindPredecessor(target_id uint64, reply *NodeInfo) error {
	*reply = node.FindPredecessor(target_id)
	return nil
}

// RPC methods for data storage.
func (node *Node) RPCGetPair(key DataPair, reply *StringBoolPair) error {
	node.NodeInfoLock.RLock()
	logrus.Infof("getpair in %s with pair %d %d %s", node.Addr, key.Node, key.Key, key.Value)
	node.NodeInfoLock.RUnlock()
	reply.Value, reply.Ok = node.GetPair(key)
	return nil
}

func (node *Node) RPCPutPair(pair DataPair, _ *struct{}) error {
	node.NodeInfoLock.RLock()
	logrus.Infof("putpair in %s with pair %d %d %s", node.Addr, pair.Node, pair.Key, pair.Value)
	node.NodeInfoLock.RUnlock()
	node.PutPair(pair)
	return nil
}

func (node *Node) RPCDeletePair(pair DataPair, flag *bool) error {
	node.NodeInfoLock.RLock()
	logrus.Infof("deletepair in %s with pair %d %d %s", node.Addr, pair.Node, pair.Key, pair.Value)
	node.NodeInfoLock.RUnlock()
	_, ok := node.DeletePair(pair)
	*flag = ok
	return nil
}

func (node *Node) RPCPutCopy(pair MapIntPair, _ *struct{}) error {
	node.PutCopy(pair)
	return nil
}

func (node *Node) RPCSplitCopy(target_id uint64, pair *MapIntPair) error {
	node.NodeInfoLock.RLock()
	self_id := FNV1aHash(node.Addr)
	node.NodeInfoLock.RUnlock()
	pair.Map = node.SplitCopy(target_id, self_id)
	return nil
}

func (node *Node) RPCMergeCopy(start_id uint64, _ *struct{}) error {
	//Return the nodeinfo
	node.NodeInfoLock.RLock()
	self_id := FNV1aHash(node.Addr)
	node.NodeInfoLock.RUnlock()
	//Merge the data between the target and self into self, which indicates the disappear of nodes.
	node.MergeCopy(start_id, self_id)
	return nil
}

// Routing methods.
func (node *Node) FindPredecessor(target_id uint64) NodeInfo {
	var cursor NodeInfo
	node.NodeInfoLock.RLock()
	cursor.Addr = node.Addr
	cursor.SuccessorList = make([]string, ListSize)
	copy(cursor.SuccessorList, node.SuccessorList)
	node.NodeInfoLock.RUnlock()
	for !IsBetween(FNV1aHash(cursor.Addr), FNV1aHash(cursor.SuccessorList[0]), target_id) {
		node.RemoteCall(cursor.Addr, "Node.FindClosestPredecessor", target_id, &cursor.Addr)
		node.RemoteCall(cursor.Addr, "Node.GetNodeInfo", "", &cursor)
	}
	logrus.Infof("Predecessor for %d is %v", target_id, cursor)
	return cursor
}
func (node *Node) FindSuccessor(target_id uint64) string {
	predecessor := node.FindPredecessor(target_id)
	return predecessor.SuccessorList[0]
}

// Stablize procotrol.
func (node *Node) Stablize() {
	node.NodeInfoLock.RLock()
	logrus.Infof("Stablize by %s", node.Addr)
	current_addr := node.Addr
	current_successor := NodeInfo{
		Addr: node.SuccessorList[0],
	}
	node.NodeInfoLock.RUnlock()
	if current_successor.Addr == "" {
		//The node has not entered the net.
		return
	}
	err := node.RemoteCall(current_successor.Addr, "Node.GetNodeInfo", "", &current_successor)
	if err != nil {
		node.FixSuccessorList()
		return
	}
	if current_successor.Predecessor != "" && current_successor.Predecessor != current_addr && IsBetween(FNV1aHash(current_addr), FNV1aHash(current_successor.Addr), FNV1aHash(current_successor.Predecessor)) {
		node.NodeInfoLock.Lock()
		for cursor := 5; cursor > 0; cursor-- {
			node.SuccessorList[cursor] = node.SuccessorList[cursor-1]
		}
		node.SuccessorList[0] = current_successor.Predecessor
		node.NodeInfoLock.Unlock()
		current_successor.Addr = current_successor.Predecessor
	}
	node.RemoteCall(current_successor.Addr, "Node.Notify", current_addr, nil)
	node.FixSuccessorList()
}

func (node *Node) FixSuccessorList() {
	flag := false
	cursor := 0
	var current_node NodeInfo
	var current_successor NodeInfo
	node.NodeInfoLock.RLock()
	current_node.Addr = node.Addr
	current_node.SuccessorList = node.SuccessorList
	node.NodeInfoLock.RUnlock()
	for ; cursor < ListSize && current_node.SuccessorList[cursor] != "" && !flag; cursor++ {
		node.RemoteCall(current_node.SuccessorList[cursor], "Node.Pong", "", &flag)
	}
	cursor--
	if cursor != 0 && current_node.SuccessorList[cursor] != "" {
		node.RemoteCall(current_node.SuccessorList[cursor], "Node.RPCMergeCopy", FNV1aHash(current_node.Addr), nil)
	}
	node.RemoteCall(current_node.SuccessorList[cursor], "Node.GetNodeInfo", "", &current_successor)
	node.NodeInfoLock.Lock()
	node.SuccessorList[0] = current_node.SuccessorList[cursor]
	for i := 1; i < ListSize && current_successor.SuccessorList[i-1] != ""; i++ {
		node.SuccessorList[i] = current_successor.SuccessorList[i-1]
	}
	node.NodeInfoLock.Unlock()
}

func (node *Node) PushCopies() {
	var current_node NodeInfo
	node.NodeInfoLock.RLock()
	current_node.Addr = node.Addr
	current_node.SuccessorList = node.SuccessorList
	node.NodeInfoLock.RUnlock()
	node.DataLock.RLock()
	self_id := FNV1aHash(current_node.Addr)
	logrus.Infof("The map in %s is %v", current_node.Addr, node.Data[self_id])
	self_data := MapIntPair{
		Map:  make(map[uint64]string),
		Node: self_id,
	}
	for k, v := range node.Data[self_id] {
		self_data.Map[k] = v
	}
	logrus.Infof("The copy is %v", self_data)
	node.DataLock.RUnlock()
	for cursor := 0; cursor < ListSize && current_node.SuccessorList[cursor] != "" && current_node.SuccessorList[cursor] != current_node.Addr; cursor++ {
		node.RemoteCall(current_node.SuccessorList[cursor], "Node.RPCPutCopy", self_data, nil)
	}
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
	target_id := (FNV1aHash(node.Addr) + 1<<i) % RingSize
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

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.Addr = addr
	node.Data = make(map[uint64]map[uint64]string)
	node.Data[FNV1aHash(addr)] = make(map[uint64]string)
	node.SuccessorList = make([]string, ListSize)
	node.FingersTable = make([]string, 64)
}

func (node *Node) Run() {
	node.Online = 1
	go node.RunRPCServer()
}

func (node *Node) BackGroundStart() {
	node.Wait.Add(3)
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
	/*go func() {
		defer node.Wait.Done()
		for atomic.LoadUint32(&node.Online) == 1 {
			node.PushCopies()
			time.Sleep(1 * time.Second)
		}
	}()*/
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
	node.BackGroundStart()
}

func (node *Node) Join(addr string) bool {
	node_info := NodeInfo{
		Addr: addr,
	}
	node.NodeInfoLock.RLock()
	logrus.Infof("Join %s, which hash is %d, through %s, which hash is %d", node.Addr, FNV1aHash(node.Addr), addr, FNV1aHash(addr))
	target_id := FNV1aHash(node.Addr)
	node.NodeInfoLock.RUnlock()
	node.RemoteCall(addr, "Node.RPCFindPredecessor", target_id, &node_info)
	logrus.Infof("1 %v", node_info)
	node.NodeInfoLock.Lock()
	node.Predecessor = node_info.Addr
	node_info.Addr = node.Addr
	copy(node.SuccessorList, node_info.SuccessorList)
	logrus.Infof("the info in new node is %s %s %v", node.Addr, node.Predecessor, node.SuccessorList)
	node.NodeInfoLock.Unlock()
	node.RemoteCall(node_info.SuccessorList[0], "Node.Notify", node_info.Addr, nil)
	node.TableLock.Lock()
	for cursor := 0; cursor < 64; cursor++ {
		node.FingersTable[cursor] = node_info.SuccessorList[0]
	}
	node.TableLock.Unlock()
	node_data := MapIntPair{
		Map:  make(map[uint64]string),
		Node: target_id,
	}
	node.RemoteCall(node_info.SuccessorList[0], "Node.RPCSplitCopy", target_id, &node_data)
	node.DataLock.Lock()
	node.Data[node_data.Node] = node_data.Map
	node.DataLock.Unlock()
	node.BackGroundStart()
	return true
}

func (node *Node) Quit() {
	if atomic.LoadUint32(&node.Online) == 1 {
		logrus.Infof("Quit %s", node.Addr)
		node.NodeInfoLock.RLock()
		current_node := node.Addr
		current_predecessor := NodeInfo{
			Addr:          node.Predecessor,
			SuccessorList: node.SuccessorList,
		}
		current_successor := NodeInfo{
			Addr:        node.SuccessorList[0],
			Predecessor: node.Predecessor,
		}
		node.NodeInfoLock.RUnlock()
		node.PushCopies()
		node.RemoteCall(current_successor.Addr, "Node.RPCMergeCopy", FNV1aHash(current_node), nil)
		node.RemoteCall(current_predecessor.Addr, "Node.ChangeNodeInfo", current_predecessor, nil)
		node.RemoteCall(current_successor.Addr, "Node.ChangeNodeInfo", current_successor, nil)
		atomic.StoreUint32(&node.Online, 0)
		node.StopRPCServer()
		node.Wait.Wait()
	}
}

func (node *Node) ForceQuit() {
	if atomic.LoadUint32(&node.Online) == 1 {
		logrus.Info("ForceQuit")
		atomic.StoreUint32(&node.Online, 0)
		node.StopRPCServer()
		node.Wait.Wait()
	}
}

// DHT methods
func (node *Node) Get(key string) (bool, string) {
	target_id := FNV1aHash(key)
	node.NodeInfoLock.RLock()
	logrus.Infof("Get %s from %s", key, node.Addr)
	self_id := FNV1aHash(node.Addr)
	node.NodeInfoLock.RUnlock()
	if IsBetween(FNV1aHash(node.Predecessor), self_id, target_id) {
		key := DataPair{
			Node: self_id,
			Key:  target_id,
		}
		logrus.Infof("Get from self %d", self_id)
		value, ok := node.GetPair(key)
		return ok, value
	}
	successor := node.FindSuccessor(target_id)
	key_info := DataPair{
		Node: FNV1aHash(successor),
		Key:  target_id,
	}
	var result StringBoolPair
	node.RemoteCall(successor, "Node.RPCGetPair", key_info, &result)
	return result.Ok, result.Value
}

func (node *Node) Put(key string, value string) bool {
	node.NodeInfoLock.RLock()
	logrus.Infof("Put %s %s from %s, with hash of %d, %d", key, value, node.Addr, FNV1aHash(key), FNV1aHash(value))
	self_id := FNV1aHash(node.Addr)
	self_predecessor := FNV1aHash(node.Predecessor)
	node.NodeInfoLock.RUnlock()
	target_id := FNV1aHash(key)
	if IsBetween(self_predecessor, self_id, target_id) {
		logrus.Infof("Put in self")
		target := DataPair{
			self_id,
			target_id,
			value,
		}
		node.PutPair(target)
		for cursor := 0; cursor < 2 && node.SuccessorList[cursor] != ""; cursor++ {
			node.RemoteCall(node.SuccessorList[cursor], "Node.RPCPutPair", target, nil)
		}
		return true
	}
	successor := node.FindSuccessor(target_id)
	var successor_info NodeInfo
	node.RemoteCall(successor, "Node.GetNodeInfo", "", &successor_info)
	target := DataPair{
		Node:  FNV1aHash(successor),
		Key:   target_id,
		Value: value,
	}
	node.RemoteCall(successor_info.Addr, "Node.RPCPutPair", target, nil)
	for cursor := 0; cursor < 2 && successor_info.SuccessorList[cursor] != ""; cursor++ {
		current_cursor := cursor
		go func(addr string, pair DataPair) {
			node.RemoteCall(addr, "Node.RPCPutPair", pair, nil)
		}(successor_info.SuccessorList[current_cursor], target)
	}
	return true
}

func (node *Node) Delete(key string) bool {
	target_id := FNV1aHash(key)
	flag := false
	node.NodeInfoLock.RLock()
	logrus.Infof("Delete %s from %s", key, node.Addr)
	current_node := NodeInfo{
		Addr:          node.Addr,
		SuccessorList: make([]string, ListSize),
	}
	copy(current_node.SuccessorList, node.SuccessorList)
	node.NodeInfoLock.RUnlock()
	self_id := FNV1aHash(current_node.Addr)
	if IsBetween(FNV1aHash(node.Predecessor), self_id, target_id) {
		target := DataPair{
			Node: self_id,
			Key:  target_id,
		}
		_, flag := node.DeletePair(target)
		for cursor := 0; cursor < 2 && node.SuccessorList[cursor] != ""; cursor++ {
			current_cursor := cursor
			var flag bool
			go func(addr string, pair DataPair) {
				node.RemoteCall(addr, "Node.RPCDeletePair", pair, &flag)
			}(current_node.SuccessorList[current_cursor], target)
		}
		return flag
	}
	successor := node.FindSuccessor(target_id)
	var successor_info NodeInfo
	node.RemoteCall(successor, "Node.GetNodeInfo", "", &successor_info)
	target := DataPair{
		Node: FNV1aHash(successor),
		Key:  target_id,
	}
	node.RemoteCall(successor_info.Addr, "Node.RPCDeletePair", target, &flag)
	for cursor := 0; cursor < 2 && successor_info.SuccessorList[cursor] != ""; cursor++ {
		current_cursor := cursor
		var flag bool
		go func(addr string, pair DataPair) {
			node.RemoteCall(addr, "Node.RPCDeletePair", pair, &flag)
		}(successor_info.SuccessorList[current_cursor], target)
	}
	return flag
}

// Test functions:
func (node *Node) CheckRing() {
	node.NodeInfoLock.RLock()
	start := node.Addr
	cursor := NodeInfo{
		Addr:          node.Addr,
		SuccessorList: make([]string, ListSize),
	}
	copy(cursor.SuccessorList, node.SuccessorList)
	node.NodeInfoLock.RUnlock()
	i := 0
	for i == 0 || cursor.Addr != start {
		node.RemoteCall(cursor.SuccessorList[0], "Node.GetNodeInfo", "", &cursor)
		i++
		logrus.Infof("CheckRing: The %d node is %s", i, cursor.Addr)
	}
}

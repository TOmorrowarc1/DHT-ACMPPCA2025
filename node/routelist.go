package node

import (
	"math/rand"
	"sync"
)

type RouteList struct {
	Addr       string
	SelfID     uint64
	Buckets    [HashBits][KValue]string
	HasRefresh [HashBits]bool
	ListLock   sync.RWMutex
}

func (list *RouteList) Init(self_addr string) {
	list.ListLock.Lock()
	defer list.ListLock.Unlock()
	list.Addr = self_addr
	list.SelfID = FNV1aHash(self_addr)
}

func (list *RouteList) SelfInfo() string {
	list.ListLock.RLock()
	defer list.ListLock.RUnlock()
	return list.Addr
}

func (list *RouteList) FindClosestNodes(target_id uint64) []string {
	list.ListLock.RLock()
	defer list.ListLock.RUnlock()
	result := make([]string, KValue)
	result_distance := make([]uint64, KValue)
	distance := target_id ^ list.SelfID
	result_length := 0
	bucket_cursor := 0
	for (1<<bucket_cursor) <= distance && bucket_cursor < HashBits-1 {
		bucket_cursor++
	}
	for _, addr := range list.Buckets[bucket_cursor] {
		if addr == "" {
			break
		}
		result[result_length] = addr
		result_distance[result_length] = target_id ^ FNV1aHash(addr)
		result_length++
	}
	var candidate_distance uint64
	if result_length != KValue {
		for cursor, buckets := range list.Buckets {
			if cursor == bucket_cursor {
				continue
			}
			for _, address := range buckets {
				if address == "" {
					break
				}
				candidate_distance = target_id ^ FNV1aHash(address)
				for empty := result_length; empty < KValue; empty++ {
					if result_distance[empty] == 0 || result_distance[empty] > candidate_distance {
						result[empty] = address
						result_distance[empty] = candidate_distance
						break
					}
				}
			}
		}
	}
	//logrus.Infof("The node %s FindClosestNodes for %d, get %v", list.Addr, target_id, result)
	return result
}

func (list *RouteList) Acknowledge(new_addr string) {
	list.ListLock.Lock()
	defer list.ListLock.Unlock()
	if new_addr == list.Addr {
		return
	}
	target_id := FNV1aHash(new_addr)
	distance := target_id ^ list.SelfID
	bucket_cursor := 0
	for (1<<bucket_cursor) <= distance && bucket_cursor < HashBits-1 {
		bucket_cursor++
	}
	for count := 0; count < KValue; count++ {
		if list.Buckets[bucket_cursor][count] == "" || list.Buckets[bucket_cursor][count] == new_addr {
			for cursor := count; cursor > 0; cursor-- {
				list.Buckets[bucket_cursor][cursor] = list.Buckets[bucket_cursor][cursor-1]
			}
			list.Buckets[bucket_cursor][0] = new_addr
			break
		}
	}
	list.HasRefresh[bucket_cursor] = true
}

func (list *RouteList) RefreshID() uint64 {
	list.ListLock.RLock()
	defer list.ListLock.RUnlock()
	var candidate []int
	for cursor := 0; cursor < HashBits; cursor++ {
		if !list.HasRefresh[cursor] {
			candidate = append(candidate, cursor)
		}
	}
	length := len(candidate)
	if length == 0 {
		return 0
	}
	result := rand.Intn(length)
	return list.SelfID ^ Uint64n(1<<candidate[result], 1<<(candidate[result]+1)-1)
}

func (list *RouteList) EraseAddress(target string) {
	list.ListLock.Lock()
	defer list.ListLock.Unlock()
	target_id := FNV1aHash(target)
	distance := target_id ^ list.SelfID
	bucket_cursor := 0
	for (1<<bucket_cursor) <= distance && bucket_cursor < HashBits-1 {
		bucket_cursor++
	}
	var count int
	for count = 0; count < KValue; count++ {
		if list.Buckets[bucket_cursor][count] == target {
			for cursor := count; cursor < KValue-1; cursor++ {
				list.Buckets[bucket_cursor][cursor] = list.Buckets[bucket_cursor][cursor+1]
			}
			break
		}
	}
	list.Buckets[bucket_cursor][KValue-1] = ""
}

func (list *RouteList) ReturnAll() []string {
	var result []string
	list.ListLock.RLock()
	defer list.ListLock.RUnlock()
	for _, bucket := range list.Buckets {
		for _, node := range bucket {
			if node == "" {
				break
			}
			result = append(result, node)
		}
	}
	return result
}

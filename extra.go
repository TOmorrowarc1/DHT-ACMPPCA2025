package main

import (
	"math/rand"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

func ConsistencyTest() (bool, int, int) {
	yellow.Println("Start Consistency Test")

	ConsistencyFailedCnt, ConsistencyTotalCnt, panicked := 0, 0, false

	defer func() {
		if r := recover(); r != nil {
			red.Println("Program panicked with", r)
		}
		panicked = true
	}()

	nodes := new([ConsisNodeSize + 1]dhtNode)
	nodeAddresses := new([ConsisNodeSize + 1]string)
	kvMap := make(map[string]string)
	nodesInNetwork := make([]int, 0, ConsisNodeSize+1)

	/* Run all nodes. */
	wg = new(sync.WaitGroup)
	for i := 0; i <= ConsisNodeSize; i++ {
		nodes[i] = NewNode(firstPort + i)
		nodeAddresses[i] = portToAddr(localAddress, firstPort+i)

		wg.Add(1)
		go nodes[i].Run(wg)
	}
	time.Sleep(ConsisAfterRunSleepTime)

	/* Node 0 creates a new network. All notes join the network. */
	joinInfo := testInfo{
		msg:       "Consistency Test join",
		failedCnt: 0,
		totalCnt:  0,
	}
	nodes[0].Create()
	nodesInNetwork = append(nodesInNetwork, 0)
	cyan.Printf("Start joining\n")
	for i := 1; i <= ConsisNodeSize; i++ {
		addr := nodeAddresses[rand.Intn(i)]
		if !nodes[i].Join(addr) {
			joinInfo.fail()
		} else {
			joinInfo.success()
		}
		nodesInNetwork = append(nodesInNetwork, i)

		time.Sleep(ConsisJoinSleepTime)
	}
	joinInfo.finish(&ConsistencyFailedCnt, &ConsistencyTotalCnt)

	time.Sleep(ConsisAfterJoinSleepTime)

	/* Put. */
	putInfo := testInfo{
		msg:       "Consistency put",
		failedCnt: 0,
		totalCnt:  0,
	}
	cyan.Printf("Start putting\n")
	for i := 0; i < ConsisPutSize; i++ {
		key := randString(lengthOfKeyValue)
		value := randString(lengthOfKeyValue)
		kvMap[key] = value

		if !nodes[rand.Intn(ConsisNodeSize+1)].Put(key, value) {
			putInfo.fail()
		} else {
			putInfo.success()
		}
	}
	putInfo.finish(&ConsistencyFailedCnt, &ConsistencyTotalCnt)

	/* Get, part 1. */
	get1Info := testInfo{
		msg:       "Get before delete",
		failedCnt: 0,
		totalCnt:  0,
	}
	cyan.Printf("Start getting\n")
	for key, value := range kvMap {
		ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get(key)
		if !ok || res != value {
			logrus.Infof("failed get with key %s,value %s should be %s", key, res, value)
			get1Info.fail()
		} else {
			get1Info.success()
		}
	}
	get1Info.finish(&ConsistencyFailedCnt, &ConsistencyTotalCnt)

	/* Delete, part 1. */
	delete1Info := testInfo{
		msg:       "Delete",
		failedCnt: 0,
		totalCnt:  0,
	}
	cyan.Printf("Start deleting\n")
	for i := 1; i <= ConsisDeleteSize; i++ {
		for key := range kvMap {
			if kvMap[key] != "delete" {
				kvMap[key] = "delete"
				success := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Delete(key)
				if !success {
					delete1Info.fail()
				} else {
					delete1Info.success()
				}
				break
			}
		}
	}
	delete1Info.finish(&ConsistencyFailedCnt, &ConsistencyTotalCnt)

	/* Get all data. */
	getInfo := testInfo{
		msg:       "Get after delete",
		failedCnt: 0,
		totalCnt:  0,
	}
	cyan.Printf("Start getting\n")
	for key, value := range kvMap {
		ok, res := nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Get(key)
		if res != "delete" {
			if !ok || res != value {
				getInfo.fail()
			} else {
				getInfo.success()
			}
		} else {
			if ok {
				logrus.Infof("failed on key %s with the value %s", key, res)
				getInfo.fail()
			} else {
				getInfo.success()
			}
		}
	}
	getInfo.finish(&ConsistencyFailedCnt, &ConsistencyTotalCnt)

	/* All nodes quit. */
	for i := 0; i <= ConsisNodeSize; i++ {
		nodes[i].Quit()
	}

	return panicked, ConsistencyFailedCnt, ConsistencyTotalCnt
}

func RealityTest() {}

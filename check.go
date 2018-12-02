package main

import (
	"fmt"
	"strconv"
)

const (
	dcPrefix    = "dc_"
	defaultDC   = "dc_default"
	rackPrefix  = "rack_"
	defaultRack = "rack_default"
	maxStoreNum = 10000000
)

/*
label部分协议：
	dc_* 标识dc，默认dc_default，即所有dc_default视为同一个dc
	rack_* 标识rack，默认rack_default，即在同一个dc的所有rack_default视为同一个rack
*/
type Store struct {
	ID             int
	LocationLabels []string
}

type Region struct {
	Replicas []int
}

type Strategy struct {
	//副本数
	ReplicaNum int
	//包含最多副本的DC与包含最少副本的DC之间副本数的最大差异
	MaxDiffDC int
	//同一个DC内，包含最多副本的rack与包含最少副本的rack之间副本数的最大差异
	MaxDiffRack int
}

//记录每一个rack内的副本数
type StoreReplica struct {
	replicas int
	storeId  int
}

//记录region一个dc内的分布情况
type DC struct {
	//该region在dc内的副本数
	ReplicaNum int
	//记录每一个rack内的副本数
	Distribution map[string]*StoreReplica
}

//由源DC向目标DC移动一个副本，在源DC选取副本最多的rack，在目标DC选择副本数最少的rack
func InterDCMove(source, target *DC) {
	maxRackLabel := ""
	maxNumInSource := 0
	for key, value := range source.Distribution {
		if value.replicas > maxNumInSource {
			maxNumInSource = value.replicas
			maxRackLabel = key
		}
	}

	minRackLabel := ""
	minNumInSource := maxStoreNum
	for key, value := range target.Distribution {
		if value.replicas < minNumInSource {
			minNumInSource = value.replicas
			minRackLabel = key
		}
	}

	source.Distribution[maxRackLabel].replicas--
	source.ReplicaNum--

	target.Distribution[minRackLabel].replicas++
	target.ReplicaNum++
}

//DC内部移动rack上的副本，由副本最多的rack移动一个副本，到副本数最少的rack
//返回值：不需要再移动，返回true；否则返回false
func IntraDCMove(dc *DC, maxDiff int) bool {
	maxRackLabel := ""
	maxNumInSource := 0
	minRackLabel := ""
	minNumInSource := maxStoreNum
	for key, value := range dc.Distribution {
		if value.replicas > maxNumInSource {
			maxNumInSource = value.replicas
			maxRackLabel = key
		}
		if value.replicas < minNumInSource {
			minNumInSource = value.replicas
			minRackLabel = key
		}
	}

	if maxNumInSource-minNumInSource <= maxDiff {
		return true
	}

	dc.Distribution[maxRackLabel].replicas--
	dc.Distribution[minRackLabel].replicas++
	return false
}

//从副本最多的rack上删除一个副本
func removeReplica(dc *DC) {
	maxRackLabel := ""
	maxNumInSource := 0
	for key, value := range dc.Distribution {
		if value.replicas > maxNumInSource {
			maxNumInSource = value.replicas
			maxRackLabel = key
		}
	}

	dc.Distribution[maxRackLabel].replicas--
	dc.ReplicaNum--
}

//dcMap -> region
func DCMapToRegion(dcMap map[string]*DC) Region {
	region := Region{make([]int, 0)}
	for _, dc := range dcMap {
		for _, distribution := range dc.Distribution {
			for i := 0; i < distribution.replicas; i++ {
				region.Replicas = append(region.Replicas, distribution.storeId)
			}
		}
	}
	return region
}

func Check(stores []Store, region Region, strategy Strategy) Region {
	if len(stores) == 0 {
		return region
	}
	currentReplicas := len(region.Replicas)
	//补足缺失的副本
	for i := 0; i < strategy.ReplicaNum-currentReplicas; i++ {
		region.Replicas = append(region.Replicas, stores[0].ID)
	}

	//dc_label -> dc
	dcMap := make(map[string]*DC)
	//storeId -> store
	idMap := make(map[int]*Store)

	//初始化dcMap
	for i := 0; i < len(stores); i++ {
		idMap[stores[i].ID] = &stores[i]

		dcLabel := defaultDC
		rackLabel := defaultRack
		for _, label := range stores[i].LocationLabels {
			if len(label) > len(dcPrefix) && label[0:len(dcPrefix)] == dcPrefix {
				dcLabel = label
				break
			}
		}
		_, ok := dcMap[dcLabel]
		if !ok {
			dcMap[dcLabel] = &DC{0, make(map[string]*StoreReplica)}
		}

		for _, label := range stores[i].LocationLabels {
			if len(label) > len(rackPrefix) && label[0:len(rackPrefix)] == rackPrefix {
				rackLabel = label
				break
			}
		}
		dcMap[dcLabel].Distribution[rackLabel] = &StoreReplica{0, stores[i].ID}
	}

	//将region信息写入dcMap
	for _, storeId := range region.Replicas {
		store, ok := idMap[storeId]
		if !ok {
			fmt.Printf("undefined storeId:" + strconv.Itoa(storeId) + " in region")
			return region
		}

		dcLabel := defaultDC
		rackLabel := defaultRack
		for _, label := range store.LocationLabels {
			if len(label) > len(dcPrefix) && label[0:len(dcPrefix)] == dcPrefix {
				dcLabel = label
				break
			}
		}

		for _, label := range store.LocationLabels {
			if len(label) > len(rackPrefix) && label[0:len(rackPrefix)] == rackPrefix {
				rackLabel = label
				break
			}
		}
		dcMap[dcLabel].ReplicaNum++
		dcMap[dcLabel].Distribution[rackLabel].replicas++
	}

	//均衡副本在DC之间的分布
	for {
		var maxDC, minDC *DC
		maxDCReplica := 0
		minDCReplica := maxStoreNum
		for _, dc := range dcMap {
			if dc.ReplicaNum > maxDCReplica {
				maxDCReplica = dc.ReplicaNum
				maxDC = dc
			}
			if dc.ReplicaNum < minDCReplica {
				minDCReplica = dc.ReplicaNum
				minDC = dc
			}
		}

		if currentReplicas > strategy.ReplicaNum {
			//删除一个副本
			removeReplica(maxDC)
			currentReplicas--
		} else if maxDCReplica-minDCReplica > strategy.MaxDiffDC {
			//移动一个副本
			InterDCMove(maxDC, minDC)
		} else {
			//调整完成
			break
		}
	}

	//均衡副本在DC内部，rack之间的分布
	for _, dc := range dcMap {
		finish := false
		for !finish {
			finish = IntraDCMove(dc, strategy.MaxDiffRack)
		}
	}

	return DCMapToRegion(dcMap)
}

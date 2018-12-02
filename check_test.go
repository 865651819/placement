package main

import (
	"fmt"
	"sort"
	"strconv"
	"testing"
)

func MakeStores1DC() []Store {
	stores := []Store{
		{0, []string{"dc_1", "rack_1"}},
		{1, []string{"dc_1", "rack_2"}},
		{2, []string{"dc_1", "rack_3"}},
		{3, []string{"dc_1", "rack_4"}},
	}
	return stores
}

func MakeStores2DC() []Store {
	stores := []Store{
		{0, []string{"dc_1", "rack_1"}},
		{1, []string{"dc_1", "rack_2"}},
		{2, []string{"dc_1", "rack_3"}},
		{3, []string{"dc_2", "rack_1"}},
		{4, []string{"dc_2", "rack_2"}},
		{5, []string{"dc_2", "rack_3"}},
		{6, []string{"dc_2", "rack_4"}},
		{7, []string{"dc_2", "rack_5"}},
	}
	return stores
}

func MakeStores3DC() []Store {
	stores := []Store{
		{0, []string{"dc_1", "rack_1"}},
		{1, []string{"dc_1", "rack_2"}},
		{2, []string{"dc_1", "rack_3"}},
		{3, []string{"dc_2", "rack_1"}},
		{4, []string{"dc_2", "rack_2"}},
		{5, []string{"dc_2", "rack_3"}},
		{6, []string{"dc_3", "rack_1"}},
		{7, []string{"dc_3", "rack_2"}},
		{8, []string{"dc_3", "rack_3"}},
		{9, []string{"dc_3", "rack_4"}},
	}
	return stores
}

func MakeDefaultStores3DC() []Store {
	stores := []Store{
		{0, []string{"rack_1"}},
		{1, []string{"rack_2"}},
		{2, []string{}},
		{3, []string{"dc_2", "rack_1"}},
		{4, []string{"dc_2", "rack_2"}},
		{5, []string{"dc_2"}},
		{6, []string{"dc_3", "rack_1"}},
		{7, []string{"dc_3", "rack_2"}},
		{8, []string{"dc_3", "rack_3"}},
		{9, []string{"dc_3", "rack_4"}},
	}
	return stores
}

func MakeSimpleStores() []Store {
	stores := []Store{
		{0, []string{"dc_1", "rack_1"}},
		{1, []string{"dc_2", "rack_1"}},
		{2, []string{"dc_2", "rack_2"}},
		{3, []string{"dc_2", "rack_3"}},
	}
	return stores
}

func verify(t *testing.T, region, newRegion, unchanged Region) {
	sort.Ints(newRegion.Replicas)
	sort.Ints(unchanged.Replicas)

	fmt.Print("region: [")
	for i := 0; i < len(region.Replicas); i++ {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(strconv.Itoa(region.Replicas[i]))
	}
	fmt.Println("]")
	fmt.Print("newRegion: [")
	for i := 0; i < len(newRegion.Replicas); i++ {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(strconv.Itoa(newRegion.Replicas[i]))
	}
	fmt.Println("]")
	fmt.Println()

	if len(unchanged.Replicas) != len(newRegion.Replicas) {
		t.Error("length not equal")
	}
	for i := 0; i < len(unchanged.Replicas); i++ {
		if unchanged.Replicas[i] != newRegion.Replicas[i] {
			t.Error("elements not error")
		}
	}
}

//===============================================  1DC  ========================================================

//3副本在不同rack(1DC)
func Test_1DC_1(t *testing.T) {
	stores := MakeStores1DC()
	region := Region{[]int{0, 1, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//2副本在同一个rack(1DC)
func Test_1DC_2(t *testing.T) {
	stores := MakeStores1DC()
	region := Region{[]int{1, 1, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个rack(1DC)
func Test_1DC_3(t *testing.T) {
	stores := MakeStores1DC()
	region := Region{[]int{2, 2, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//===============================================  2DC 3副本 ========================================================

//3副本均衡(2DC)
func Test_2DC_1(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{0, 1, 3}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本均衡(2DC)
func Test_2DC_2(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{1, 3, 4}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个DC，不同的rack(2DC)
func Test_2DC_3(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{0, 1, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个DC，2副本在不同的rack(2DC)
func Test_2DC_4(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{1, 1, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个DC，同一个rack(2DC)
//期望：一个副本移至另一个DC，另一个副本移至同一个DC的另一个rack
func Test_2DC_5(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{1, 1, 1}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//===============================================  2DC 5副本 ========================================================

//5副本均衡(2DC)
func Test_2DC_5replica_1(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{0, 1, 3, 4, 5}}
	strategy := Strategy{5, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//5副本，4副本在一同个DC(2DC)
func Test_2DC_5replica_2(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{0, 3, 4, 5, 6}}
	strategy := Strategy{5, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//5副本，DC均衡，2副本在同一个rack(2DC)
func Test_2DC_5replica_3(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{2, 2, 4, 5, 6}}
	strategy := Strategy{5, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//5副本，DC均衡，3副本在同一个rack(2DC)
func Test_2DC_5replica_4(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{2, 2, 5, 5, 5}}
	strategy := Strategy{5, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//5副本同一个DC(2DC)
func Test_2DC_5replica_5(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{3, 3, 4, 6, 6}}
	strategy := Strategy{5, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//5副本同一个DC，同一个rack(2DC)
func Test_2DC_5replica_6(t *testing.T) {
	stores := MakeStores2DC()
	region := Region{[]int{2, 2, 2, 2, 2}}
	strategy := Strategy{5, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//===============================================  3DC  ========================================================

//3副本均衡(3DC)
func Test_3DC_1(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{0, 3, 6}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//2副本在同一个DC(3DC)
func Test_3DC_2(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{0, 1, 5}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//2副本在同一个rack(3DC)
func Test_3DC_3(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{0, 0, 5}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个DC(3DC)
func Test_3DC_4(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{0, 1, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个DC,同一个rack(3DC)
func Test_3DC_5(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{2, 2, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个DC,2副本在同一个rack(3DC)
func Test_3DC_6(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{1, 2, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//===============================================  缺失/冗余 ========================================================

//replica缺失
func Test_missing(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{0, 3, 6}}
	strategy := Strategy{6, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//replica冗余
func Test_redundancy(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{0, 3, 4, 6}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//===============================================  default dc/rack  ========================================================

//3副本均衡
func Test_3DC_default_1(t *testing.T) {
	stores := MakeDefaultStores3DC()
	region := Region{[]int{0, 2, 6}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//2副本在同一个DC
func Test_3DC_default_2(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{0, 1, 5}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//2副本在同一个rack
func Test_3DC_default_3(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{0, 0, 5}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个DC
func Test_3DC_default_4(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{0, 1, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个DC,同一个rack
func Test_3DC_default_5(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{2, 2, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//3副本在同一个DC,2副本在同一个rack
func Test_3DC_default_6(t *testing.T) {
	stores := MakeStores3DC()
	region := Region{[]int{1, 2, 2}}
	strategy := Strategy{3, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

//===============================================  单rack  ========================================================

func Test_2DC_simple_1(t *testing.T) {
	stores := MakeSimpleStores()
	region := Region{[]int{0, 0, 0, 0, 1}}
	strategy := Strategy{5, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

func Test_2DC_simple_2(t *testing.T) {
	stores := MakeSimpleStores()
	region := Region{[]int{0, 1, 2, 2, 3}}
	strategy := Strategy{5, 1, 1}
	newRegion := Check(stores, region, strategy)
	unchanged := Check(stores, newRegion, strategy)
	verify(t, region, newRegion, unchanged)
	t.Log("passed")
}

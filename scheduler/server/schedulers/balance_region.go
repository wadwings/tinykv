// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	var selectedRegionInfo *core.RegionInfo
	var selectedStoreIndex uint64
	StoreInfos := cluster.GetStores()

	cluster.GetMaxReplicas()
	sort.Slice(StoreInfos, func(i, j int) bool {
		return StoreInfos[i].GetRegionSize() > StoreInfos[j].GetRegionSize()
	})
	upStoreCount := 0
	for _, store := range StoreInfos {
		if IsStoreAvailable(store, cluster) {
			upStoreCount++
		}
	}
	if upStoreCount <= cluster.GetMaxReplicas() {
		//don't have spare slot for peer to move
		return nil
	}
	for i, storeInfo := range StoreInfos {
		if !IsStoreAvailable(storeInfo, cluster) {
			continue
		}
		cluster.GetPendingRegionsWithLock(storeInfo.GetID(), func(container core.RegionsContainer) {
			selectedRegionInfo = container.RandomRegion(nil, nil)
			if selectedRegionInfo == nil {
				cluster.GetFollowersWithLock(storeInfo.GetID(), func(container core.RegionsContainer) {
					selectedRegionInfo = container.RandomRegion(nil, nil)
					if selectedRegionInfo == nil {
						cluster.GetLeadersWithLock(storeInfo.GetID(), func(container core.RegionsContainer) {
							selectedRegionInfo = container.RandomRegion(nil, nil)
						})
					}
				})
			}
		})
		selectedStoreIndex = uint64(i)
		if selectedRegionInfo != nil {
			break
		}
	}
	var minStoreID uint64
	for i := uint64(len(StoreInfos) - 1); i > selectedStoreIndex; i-- {
		if !IsStoreAvailable(StoreInfos[i], cluster) {
			continue
		}
		if _, ok := selectedRegionInfo.GetStoreIds()[StoreInfos[i].GetID()]; !ok {
			minStoreID = i
			break
		}
	}
	selectedRegionInfo.GetStoreIds()
	if StoreInfos[selectedStoreIndex].GetRegionSize()-StoreInfos[minStoreID].GetRegionSize() <= 2*selectedRegionInfo.GetApproximateSize() {
		//The diff between two store region size is not big enough
		return nil
	}
	oldStoreId := StoreInfos[selectedStoreIndex].GetID()
	newStoreId := StoreInfos[minStoreID].GetID()
	var peerID uint64
	var opt *operator.Operator
	var err error
	for _, peer := range selectedRegionInfo.GetPeers() {
		if peer.StoreId == oldStoreId {
			peerID = peer.Id
			break
		}
	}
	if opt, err = operator.CreateMovePeerOperator("", cluster, selectedRegionInfo, operator.OpBalance, oldStoreId, newStoreId, peerID); err != nil {
		return nil
	} else {
		return opt
	}
}

func IsStoreAvailable(info *core.StoreInfo, cluster opt.Cluster) bool {
	if info.GetMeta().GetState() != metapb.StoreState_Up {
		return false
	}
	if info.DownTime() > cluster.GetMaxStoreDownTime() {
		return false
	}
	return true
}

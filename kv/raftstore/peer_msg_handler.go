package raftstore

import (
	"bytes"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped || !d.RaftGroup.HasReady() {
		return
	}
	//log.Infof("%v start HandleRaftReady", d.PeerId())
	ready := d.RaftGroup.Ready()
	//log.Infof("%+v", ready)
	//log.Infof("%v %+v", d.Tag, ready.Messages)
	if _, err := d.peerStorage.SaveReadyState(&ready); err != nil {
		return
	}
	d.RaftGroup.Advance(ready)
	//d.ctx.storeMeta.setRegion(d.Region(), d.peer) //TODO
	//d.handlePendingCC()
	//log.Infof("%v peer cache %+v", d.Tag, d.peerCache)
	if !raft.IsEmptySnap(&ready.Snapshot) {
		d.updateStoreMeta()
	}
	d.HandleProposal(ready.CommittedEntries)
	d.Send(d.ctx.trans, ready.Messages)

	//log.Infof("%v after handle possible confchange, peer cache %+v", d.Tag, d.peerCache)
	// Your Code Here (2B).
}

func (d *peerMsgHandler) handleConfChange(cc *eraftpb.ConfChange) {
	d.updateRegionPeer(cc)
	if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		if cc.NodeId == d.PeerId() {
			if !d.stopped {
				d.destroyPeer()
			}
			return
		}
	} else {
		d.removePeerCache(cc.NodeId)
		// clear cache
	}
	d.RaftGroup.ApplyConfChange(*cc)

}

func (d *peerMsgHandler) updateStoreMeta() {
	d.ctx.storeMeta.Lock()
	defer d.ctx.storeMeta.Unlock()
	d.ctx.storeMeta.regions[d.regionId] = d.Region()
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})

}

func (d *peerMsgHandler) IsValidSplit(split *raft_cmdpb.SplitRequest) bool {
	// check key in region only ensure splitKey is in [startKey, endKey), we need one more compare to ensure splitKey != startKey
	res := true
	if err := util.CheckKeyInRegion(split.SplitKey, d.Region()); err != nil {
		log.Warnf("Split invalid, %v current region info %+v, Split Key %+v Not in region", d.Tag, d.Region(), split.SplitKey)
		res = false
	}
	if len(split.NewPeerIds) != len(d.Region().Peers) {
		log.Warnf("Split invalid, %v current region info %+v, Split peers number %+v Not match", d.Tag, d.Region(), split.NewPeerIds)
		res = false
	}
	if bytes.Compare(split.SplitKey, d.Region().StartKey) == 0 {
		log.Warnf("Split invalid, %v current region info %+v, Split Key %+v same to startKey", d.Tag, d.Region(), split.SplitKey)
		res = false
	}
	return res

}

func (d *peerMsgHandler) updateSplitMeta(adminRequest *raft_cmdpb.AdminRequest) []byte {
	d.ctx.storeMeta.Lock()
	defer d.ctx.storeMeta.Unlock()
	split := adminRequest.Split

	//TODO multiple same Split Request
	if d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		return nil
	}
	d.Region().RegionEpoch = &metapb.RegionEpoch{
		ConfVer: d.Region().RegionEpoch.ConfVer,
		Version: d.Region().RegionEpoch.Version + 1,
	}
	endKey := d.Region().EndKey
	d.Region().EndKey = split.SplitKey

	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	d.ctx.storeMeta.regions[d.regionId] = d.Region()
	//log.Warnf("%v handle split request, current Region Info %+v", d.Tag, d.Region())
	var kvWB engine_util.WriteBatch
	meta.WriteRegionState(&kvWB, d.Region(), rspb.PeerState_Normal)
	d.peerStorage.Engines.WriteKV(&kvWB)
	log.Infof("%v save region state %+v to db", d.Tag, d.Region())
	return endKey
}

func (d *peerMsgHandler) checkSplitMessage(entries []eraftpb.Entry) {
	var adminRequest raft_cmdpb.AdminRequest
	for _, entry := range entries {
		if entry.Data != nil {
			if err := adminRequest.Unmarshal(entry.Data); err == nil && adminRequest.CmdType == raft_cmdpb.AdminCmdType_Split {
				d.updateSplitMeta(&adminRequest)
			}
		}
	}
}

func (d *peerMsgHandler) HandleNotLeaderProposal() {
	for _, proposal := range d.proposals {
		proposal.cb.Done(ErrResp(&util.ErrNotLeader{RegionId: d.regionId, Leader: d.getPeerFromCache(d.GetLead())}))
	}
	d.proposals = make([]*proposal, 0)
}

func (d *peerMsgHandler) onSplit(split *raft_cmdpb.SplitRequest, endKey []byte) *metapb.Region {
	d.ctx.storeMeta.Lock()
	defer d.ctx.storeMeta.Unlock()
	var peers []*metapb.Peer
	for i, peerID := range split.NewPeerIds {
		peers = append(peers, &metapb.Peer{
			Id:      peerID,
			StoreId: d.Region().Peers[i].StoreId,
		})
	}
	newRegion := &metapb.Region{
		StartKey: split.SplitKey,
		EndKey:   endKey,
		Id:       split.NewRegionId,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: d.Region().RegionEpoch.ConfVer,
			Version: d.Region().RegionEpoch.Version,
		},
		Peers: peers,
	}
	log.Infof("Handle Split Request, old Region %+v, new Region %+v", d.Region(), newRegion)
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	d.ctx.storeMeta.regions[newRegion.Id] = newRegion
	var kvWB engine_util.WriteBatch
	meta.WriteRegionState(&kvWB, newRegion, rspb.PeerState_Normal)
	d.peerStorage.Engines.WriteKV(&kvWB)
	log.Infof("%v save region state %+v to db", d.Tag, d.Region())

	return newRegion
}

func (d *peerMsgHandler) HandleProposal(entries []eraftpb.Entry) {
	if !d.IsLeader() {
		d.HandleNotLeaderProposal()
		d.RaftGroup.EmptySnapRequest()
		//d.checkSplitMessage(entries)
	}
	for _, entry := range entries {
		response := &raft_cmdpb.RaftCmdResponse{}
		if entry.EntryType == eraftpb.EntryType_EntryConfChange {
			var cc eraftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				response = ErrResp(err)
			} else {
				d.handleConfChange(&cc)
				response = &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					AdminResponse: &raft_cmdpb.AdminResponse{
						CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
					},
				}
			}
		} else {
			var request raft_cmdpb.Request
			var adminRequest raft_cmdpb.AdminRequest
			if err := adminRequest.Unmarshal(entry.Data); err != nil {
				if err := request.Unmarshal(entry.Data); err != nil {
					response = ErrResp(err)
				}
			}
			if request.CmdType != raft_cmdpb.CmdType_Invalid {
				response = &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					Responses: []*raft_cmdpb.Response{{
						CmdType: request.CmdType,
					}},
				}
			} else if adminRequest.CmdType != raft_cmdpb.AdminCmdType_InvalidAdmin {
				switch adminRequest.CmdType {
				case raft_cmdpb.AdminCmdType_CompactLog:
					if d.IsLeader() {
						d.ScheduleCompactLog(adminRequest.CompactLog.CompactIndex)
					}
				case raft_cmdpb.AdminCmdType_Split:
					if !d.IsValidSplit(adminRequest.Split) {
						break
					}
					endKey := d.updateSplitMeta(&adminRequest)
					if !d.IsLeader() {
						break
					}
					d.RaftGroup.EmptySnapRequest()
					split := adminRequest.Split
					newRegion := d.onSplit(split, endKey)
					peer, _ := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
					d.ctx.router.register(peer)
					t := newPeerMsgHandler(peer, d.ctx)

					d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
					t.HeartbeatScheduler(t.ctx.schedulerTaskSender)
					t.startTicker()
					//TODO ExceedEndKey not used
				}
				response = &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					AdminResponse: &raft_cmdpb.AdminResponse{
						CmdType: adminRequest.CmdType,
					},
				}
			}
		}
		if d.IsLeader() {
			proposal := d.findProposal(&entry)
			if proposal != nil {
				proposal.cb.Done(response)
			}
		}
	}
}

func (d *peerMsgHandler) findProposal(entry *eraftpb.Entry) *proposal {
	if len(d.proposals) == 0 {
		return nil
	}
	for i, proposal := range d.proposals {
		if proposal.index == entry.Index && proposal.term == entry.Term {
			d.proposals = append(d.proposals[:i], d.proposals[i+1:]...)
			return proposal
		}
	}
	return nil
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %+v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	//if req.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
	//	log.Infof("")
	//}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	response := newCmdResp()

	for _, request := range msg.Requests {
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			{

				if err := util.CheckKeyInRegion(request.Get.Key, d.Region()); err != nil {
					cb.Done(ErrResp(err))
					continue
				}
				value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, request.Get.Cf, request.Get.Key)
				if err != nil {
					cb.Done(ErrResp(err))
				} else {
					response.Responses = append(response.Responses, &raft_cmdpb.Response{
						CmdType: request.CmdType,
						Get: &raft_cmdpb.GetResponse{
							Value: value,
						},
					})
				}
				cb.Done(response)
			}
		case raft_cmdpb.CmdType_Snap:
			clonedRegion := new(metapb.Region)
			err := util.CloneMsg(d.Region(), clonedRegion)
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.RaftGroup.RegisterSnapRequest(func(err error) {
				if err != nil {
					log.Infof("%s refuse snap request", d.Tag)
					cb.Done(ErrResp(err))
					return
				}
				log.Infof("%s handle snap request", d.Tag)
				cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				cb.Done(&raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					Responses: []*raft_cmdpb.Response{{
						CmdType: request.CmdType,
						Snap:    &raft_cmdpb.SnapResponse{Region: clonedRegion},
					}},
				})
			})
		case raft_cmdpb.CmdType_Put:
			if err := util.CheckKeyInRegion(request.Put.Key, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				continue
			}
			str, _ := request.Marshal()
			d.peer.proposals = append(d.peer.proposals, &proposal{
				term:  d.Term(),
				index: d.nextProposalIndex(),
				cb:    cb,
			})
			err := d.peer.RaftGroup.Raft.Step(eraftpb.Message{
				MsgType: eraftpb.MessageType_MsgPropose,
				To:      d.GetLead(),
				Entries: []*eraftpb.Entry{{Data: str}},
			})
			if err != nil {
				cb.Done(ErrResp(err))
			}
		case raft_cmdpb.CmdType_Delete:
			if err := util.CheckKeyInRegion(request.Delete.Key, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				continue
			}
			str, _ := request.Marshal()
			d.peer.proposals = append(d.peer.proposals, &proposal{
				term:  d.Term(),
				index: d.nextProposalIndex(),
				cb:    cb,
			})
			err := d.peer.RaftGroup.Raft.Step(eraftpb.Message{
				MsgType: eraftpb.MessageType_MsgPropose,
				To:      d.GetLead(),
				Entries: []*eraftpb.Entry{{Data: str}},
			})
			if err != nil {
				cb.Done(ErrResp(err))
			}
		}
	}
	if msg.AdminRequest != nil {
		adminRequest := msg.AdminRequest
		switch adminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			str, _ := adminRequest.Marshal()
			d.peer.proposals = append(d.peer.proposals, &proposal{
				term:  d.Term(),
				index: d.nextProposalIndex(),
				cb:    cb,
			})
			err := d.peer.RaftGroup.Raft.Step(eraftpb.Message{
				MsgType: eraftpb.MessageType_MsgPropose,
				To:      d.GetLead(),
				Entries: []*eraftpb.Entry{{Data: str}},
			})
			if err != nil {
				cb.Done(ErrResp(err))
			}
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(adminRequest.TransferLeader.Peer.Id)
			cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: adminRequest.CmdType,
				},
			})
		case raft_cmdpb.AdminCmdType_ChangePeer:
			log.Warnf("%v receive change peer propose %+v", d.Tag, adminRequest)
			if adminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode && adminRequest.ChangePeer.Peer.Id == d.PeerId() {
				var transfereeId uint64
				for _, peer := range d.Region().GetPeers() {
					if peer.GetId() != d.PeerId() {
						transfereeId = peer.GetId()
						break
					}
				}
				d.RaftGroup.TransferLeader(transfereeId)
				cb.Done(ErrResp(&util.ErrNotLeader{RegionId: d.regionId}))
			} else {
				peer, _ := adminRequest.ChangePeer.Peer.Marshal()
				d.peer.proposals = append(d.peer.proposals, &proposal{
					term:  d.Term(),
					index: d.nextProposalIndex(),
					cb:    cb,
				})
				if err := d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
					ChangeType: adminRequest.ChangePeer.ChangeType,
					NodeId:     adminRequest.ChangePeer.Peer.Id,
					Context:    peer,
				}); err != nil {
					cb.Done(ErrResp(err))
				}
			}
		case raft_cmdpb.AdminCmdType_Split:
			str, _ := adminRequest.Marshal()
			d.peer.proposals = append(d.peer.proposals, &proposal{
				term:  d.Term(),
				index: d.nextProposalIndex(),
				cb:    cb,
			})
			err := d.peer.RaftGroup.Raft.Step(eraftpb.Message{
				MsgType: eraftpb.MessageType_MsgPropose,
				To:      d.GetLead(),
				Entries: []*eraftpb.Entry{{Data: str}},
			})
			if err != nil {
				cb.Done(ErrResp(err))
			}
		}

	}
}

func (d *peerMsgHandler) GetLead() uint64 {
	if d.IsLeader() {
		return d.PeerId()
	} else {
		return d.LeaderId()
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) updateRegionPeer(cc *eraftpb.ConfChange) {
	ps := d.peerStorage
	if cc.ChangeType == eraftpb.ConfChangeType_AddNode && !ps.PeerIDExists(cc.NodeId) {
		ps.region.RegionEpoch.ConfVer++

		var peer metapb.Peer
		_ = peer.Unmarshal(cc.Context)
		ps.region.Peers = append(ps.region.Peers, &peer)
		d.saveRegionInfo()
		log.Warnf("%v current peers %+v", d.Tag, d.Region().Peers)
		log.Warnf("%v current region Epoch %+v", d.Tag, d.Region().RegionEpoch)
		d.updateStoreMeta()
	} else if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && ps.PeerIDExists(cc.NodeId) {
		ps.region.RegionEpoch.ConfVer++
		for i, peer := range ps.region.Peers {
			if peer.Id == cc.NodeId {
				ps.region.Peers = append(ps.region.Peers[:i], ps.region.Peers[i+1:]...)
				break
			}
		}
		d.saveRegionInfo()
		log.Warnf("%v current peers %+v", d.Tag, d.Region().Peers)
		log.Warnf("%v current region Epoch %+v", d.Tag, d.Region().RegionEpoch)
		d.updateStoreMeta()
	}
}

func (d *peerMsgHandler) saveRegionInfo() {
	var kvWB engine_util.WriteBatch
	meta.WriteRegionState(&kvWB, d.Region(), rspb.PeerState_Normal)
	if err := d.peerStorage.Engines.WriteKV(&kvWB); err != nil {
		panic(err)
	}
	log.Infof("%v save region state %+v to db", d.Tag, d.Region())

}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		log.Error("Trying delete %v, can't delete region info in regionRanges", d.Tag)
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		log.Error("Trying delete %v, can't delete region info in meta region", d.Tag)
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

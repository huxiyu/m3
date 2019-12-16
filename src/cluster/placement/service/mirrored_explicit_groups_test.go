// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package service

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placement/selector"
	placementstorage "github.com/m3db/m3/src/cluster/placement/storage"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	zone          = "zone1"
	defaultWeight = 1
	numShards     = 12
	rf            = 2
)

var (
	logger, _ = zap.NewDevelopment()
)

// TestExplicitMirroredGrouper contains functional tests starting at the placement.Service level.
func TestExplicitMirroredGrouper(t *testing.T) {
	mustBuildInitialPlacement := func(
		t *testing.T, tctx *mirroredGrouperTestContext) placement.Placement {
		p, err := tctx.Service.BuildInitialPlacement(tctx.Instances, numShards, rf)
		require.NoError(t, err)
		assertPlacementRespectsGroups(t, p, tctx.Groups)
		return p
	}

	t.Run("BuildInitialPlacement", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)

		mustBuildInitialPlacement(t, tctx)
	})

	t.Run("BuildInitialPlacement insufficient nodes", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)
		_, err := tctx.Service.BuildInitialPlacement([]placement.Instance{
			newInstanceWithID("a"),
		}, numShards, rf)
		assert.EqualError(t, err, "found 1 count of shard set id 1, expecting 2")
		// assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	t.Run("BuildInitialPlacement given too many nodes should use only RF", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)
		p, err := tctx.Service.BuildInitialPlacement([]placement.Instance{
			newInstanceWithID("a"),
			newInstanceWithID("c"),
			newInstanceWithID("g"),
			newInstanceWithID("h"),
		}, numShards, rf)
		assert.NoError(t, err)
		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	t.Run("AddInstances", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)
		mustBuildInitialPlacement(t, tctx)

		toAdd := []placement.Instance{
			newInstanceWithID("e"),
			newInstanceWithID("f"),
		}

		p, addedInstances, err := tctx.Service.AddInstances(toAdd)
		require.NoError(t, err)
		assert.Equal(t, toAdd, addedInstances)
		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	t.Run("AddInstances given too many nodes should use only RF", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)
		mustBuildInitialPlacement(t, tctx)

		toAdd := []placement.Instance{
			newInstanceWithID("e"),
			newInstanceWithID("f"),
			newInstanceWithID("i"),
		}

		p, addedInstances, err := tctx.Service.AddInstances(toAdd)
		require.NoError(t, err)
		assert.Len(t, addedInstances, p.ReplicaFactor())
		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	t.Run("ReplaceInstances", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)
		mustBuildInitialPlacement(t, tctx)

		group1Instance := newInstanceWithID("g")
		candidates := []placement.Instance{
			group1Instance,
			newInstanceWithID("e"),
		}
		p, usedInstances, err := tctx.Service.ReplaceInstances([]string{"a"}, candidates)
		require.NoError(t, err)
		require.Len(t, usedInstances, 1)
		assert.Equal(t, group1Instance.ID(), usedInstances[0].ID())
		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	// N.B.: at time of this writing this technically doesn't need grouping, but check that
	// the grouping is respected anyhow.
	t.Run("RemoveInstances", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)
		mustBuildInitialPlacement(t, tctx)

		p, err := tctx.Service.RemoveInstances([]string{"a", "c"})
		require.NoError(t, err)

		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})
}

type mirroredGrouperTestContext struct {
	KVStore   kv.Store
	Opts      placement.Options
	Storage   placement.Storage
	Service   placement.Service
	Grouper   placement.MirroredGrouper
	Instances []placement.Instance
	Groups    map[string]string
}

func mirroredGrouperSetup(t *testing.T) *mirroredGrouperTestContext {
	tctx := &mirroredGrouperTestContext{}

	tctx.Instances = []placement.Instance{
		newInstanceWithID("a"),
		newInstanceWithID("b"),
		newInstanceWithID("c"),
		newInstanceWithID("d"),
	}

	tctx.Groups = map[string]string{
		"c": "group1",
		"a": "group1",

		// for replacement
		"g": "group1",
		"h": "group1",

		"b": "group2",
		"d": "group2",

		// additional instances
		"e": "group3",
		"f": "group3",
		"i": "group3",
	}

	tctx.Grouper = selector.NewExplicitMirroredGrouper(
		logger,
		selector.NewInstanceToGroupIDFromMap(tctx.Groups))
	tctx.Opts = placement.NewOptions().
		SetIsMirrored(true).
		SetMirroredGrouper(tctx.Grouper).
		SetValidZone(zone)

	tctx.KVStore = mem.NewStore()
	tctx.Storage = placementstorage.NewPlacementStorage(tctx.KVStore, "placement", tctx.Opts)
	tctx.Service = NewPlacementService(tctx.Storage, tctx.Opts)
	return tctx
}

// assertGroupsAreRespected checks that each group in the given group map has:
//   - the same shards assigned
//   - the same shardset ID
//
// Note: shard comparison ignores SourceID, as this is expected to differ between instances in
// a group (new instances take shards from different replicas in an existing group)
func assertPlacementRespectsGroups(t *testing.T, p placement.Placement, groups map[string]string) bool {
	// check that the groups are respected.
	instancesByGroupID := make(map[string][]placement.Instance, len(groups))

	for _, inst := range p.Instances() {
		groupID, ok := groups[inst.ID()]
		if !assert.True(t, ok, "instance %s has no group", inst.ID()) {
			return false
		}
		instancesByGroupID[groupID] = append(instancesByGroupID[groupID], inst)
	}

	rtn := true
	for groupID, groupInsts := range instancesByGroupID {
		// make sure all shards are the same.
		if !assert.True(t, len(groupInsts) >= 1, "groupID %s", groupID) {
			continue
		}
		compareInst := groupInsts[0]
		for _, inst := range groupInsts[1:] {
			instIDs := []string{inst.ID(), compareInst.ID()}

			rtn = rtn && assert.Equal(t,
				shardIDs(compareInst.Shards()), shardIDs(inst.Shards()),
				"%s (actual) != %s (expected) for instances %v",
				inst.Shards(), compareInst.Shards(),
				instIDs)
			rtn = rtn && assert.Equal(t,
				compareInst.ShardSetID(), inst.ShardSetID(),
				"shard not equal for instances %v", instIDs)
		}
	}
	return rtn
}

func shardIDs(ss shard.Shards) []uint32 {
	ids := make([]uint32, 0, len(ss.All()))
	for _, s := range ss.All() {
		ids = append(ids, s.ID())
	}
	return ids
}

func newInstanceWithID(id string) placement.Instance {
	return placement.NewEmptyInstance(
		id,
		nextIsolationGroup(),
		zone,
		fmt.Sprintf("localhost:%d", randPort()),
		defaultWeight,
	)
}

// completely random valid port, not necessarily open.
func randPort() int {
	return rand.Intn(1 << 16)
}

var curISOGroup = 0

// Not thread safe; factor into a factory object if you need that.
func nextIsolationGroup() string {
	myGroup := curISOGroup
	curISOGroup++
	return fmt.Sprintf("iso-group-%d", myGroup)
}

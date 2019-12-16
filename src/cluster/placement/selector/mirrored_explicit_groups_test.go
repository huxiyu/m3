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

package selector

import (
	"testing"

	"github.com/m3db/m3/src/cluster/placement"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	logger = zap.NewNop()
	// uncomment for logging
	// logger, _ = zap.NewDevelopment()
)

func instanceIDsForGroups(groups [][]placement.Instance) [][]string {
	idGroups := make([][]string, 0, len(groups))
	for _, group := range groups {
		idGroups = append(idGroups, instanceIDs(group))
	}
	return idGroups
}

func instanceIDs(instances []placement.Instance) []string {
	ids := make([]string, 0, len(instances))
	for _, inst := range instances {
		ids = append(ids, inst.ID())
	}
	return ids
}

// testAddingNodesBehavior contains assertions common to both GroupInitialInstances and
// GroupAddingInstances.
func testAddingNodesBehavior(
	t *testing.T,
	doAdd func(tctx *mirroredGrouperTestContext, candidates []placement.Instance) ([][]placement.Instance, error)) {
	t.Run("RF hosts in group", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)

		groups, err := doAdd(tctx, tctx.Instances)
		require.NoError(t, err)

		assert.Equal(
			t,
			[][]string{{"a", "c"}, {"b", "d"}},
			instanceIDsForGroups(groups),
		)
	})

	t.Run("too many hosts in group shortens the group to RF", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)
		tctx.Grouper = NewExplicitMirroredGrouper(
			logger,
			NewInstanceToGroupIDFromMap(map[string]string{
				"a": "group1",
				"b": "group1",
				"c": "group1",
			}))
		tctx.Placement = tctx.Placement.SetReplicaFactor(2)

		groups, err := doAdd(tctx, []placement.Instance{
			newInstanceWithID("a"),
			newInstanceWithID("b"),
			newInstanceWithID("c"),
		})
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Len(t, groups[0], 2)
	})

	t.Run("no group configured errors", func(t *testing.T) {
		tctx := mirroredGrouperSetup(t)
		_, err := doAdd(tctx, []placement.Instance{
			newInstanceWithID("a"),
			newInstanceWithID("nogroup")})
		assert.EqualError(t,
			err,
			"finding group for nogroup: instance nogroup "+
				"doesn't have a corresponding group in ID to group map")
	})

	t.Run("insufficient hosts in group is ok", func(t *testing.T) {
		// case should be handled at a higher level.
		tctx := mirroredGrouperSetup(t)
		_, err := doAdd(tctx, []placement.Instance{
			newInstanceWithID("a"),
		})
		assert.NoError(t, err)
	})
}

func TestExplicitMirroredGrouper_GroupAddingInstances(t *testing.T) {
	testAddingNodesBehavior(
		t,
		func(tctx *mirroredGrouperTestContext, candidates []placement.Instance) ([][]placement.Instance, error) {
			return tctx.Grouper.GroupAddingInstances(candidates, tctx.Placement)
		})

}

func TestExplicitMirroredGrouper_GroupInitialInstances(t *testing.T) {
	testAddingNodesBehavior(
		t,
		func(tctx *mirroredGrouperTestContext, candidates []placement.Instance) ([][]placement.Instance, error) {
			return tctx.Grouper.GroupInitialInstances(candidates, tctx.Placement.ReplicaFactor())
		})
}

func TestExplicitMirroredGrouper_GroupInstancesWithReplacements(t *testing.T) {
	type testContext struct {
		*mirroredGrouperTestContext
		ToReplace placement.Instance
	}
	setup := func(t *testing.T) *testContext {
		tctx := mirroredGrouperSetup(t)
		tctx.Placement = tctx.Placement.SetInstances(tctx.Instances)

		toReplace, ok := tctx.Placement.Instance("a")
		require.True(t, ok)

		return &testContext{
			mirroredGrouperTestContext: tctx,
			ToReplace:                  toReplace,
		}
	}

	t.Run("correct replacement", func(t *testing.T) {
		tctx := setup(t)

		instG := newInstanceWithID("g")
		groups, err := tctx.Grouper.GroupInstancesWithReplacements(
			[]placement.Instance{instG, newInstanceWithID("e")},
			[]placement.Instance{tctx.ToReplace},
			tctx.Placement,
		)
		require.NoError(t, err)

		assert.Equal(t, []placement.MirroredReplacementGroup{{
			Leaving:     tctx.ToReplace,
			Replacement: instG,
		}}, groups)
	})

	t.Run("no valid replacements", func(t *testing.T) {
		tctx := setup(t)

		_, err := tctx.Grouper.GroupInstancesWithReplacements(
			[]placement.Instance{newInstanceWithID("e"), newInstanceWithID("f")},
			[]placement.Instance{tctx.ToReplace},
			tctx.Placement,
		)
		require.EqualError(t, err,
			"leaving instance a has no valid replacements in the same group (group1)")
	})

	// TODO: more tests
}

type mirroredGrouperTestContext struct {
	Grouper   placement.MirroredGrouper
	Instances []placement.Instance
	Placement placement.Placement
	Groups    map[string]string
}

func mirroredGrouperSetup(_ *testing.T) *mirroredGrouperTestContext {
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

		"b": "group2",
		"d": "group2",

		// additional instances
		"e": "group3",
		"f": "group3",
	}

	tctx.Grouper = NewExplicitMirroredGrouper(logger, NewInstanceToGroupIDFromMap(tctx.Groups))
	tctx.Placement = placement.NewPlacement().SetReplicaFactor(2)
	return tctx
}

func newInstanceWithID(id string) placement.Instance {
	return placement.NewInstance().SetID(id)
}

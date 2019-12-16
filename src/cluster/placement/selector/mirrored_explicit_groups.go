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
	"fmt"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/x/errors"

	"go.uber.org/zap"
)

type explicitMirroredGrouper struct {
	instanceIDToGroupID InstanceToGroupID
	logger              *zap.Logger
}

// InstanceToGroupID maps an instance to its mirrored group.
type InstanceToGroupID func(inst placement.Instance) (string, error)

// NewInstanceToGroupIDFromMap creates a simple lookup function for an instances group, which
// looks up the group ID for an instance by the instance ID.
func NewInstanceToGroupIDFromMap(instanceIDToGroupID map[string]string) InstanceToGroupID {
	return func(inst placement.Instance) (string, error) {
		gid, ok := instanceIDToGroupID[inst.ID()]
		if !ok {
			return "", fmt.Errorf(
				"instance %s doesn't have a corresponding group in ID to group map",
				inst.ID(),
			)
		}
		return gid, nil
	}
}

// NewExplicitMirroredGrouper constructs a placement.MirroredGrouper which simply groups hosts
// according to their group ID (provided by instanceToGroupID). That is, instances with the
// same group ID are placed in the same group
func NewExplicitMirroredGrouper(
	logger *zap.Logger, instanceToGroupID InstanceToGroupID) placement.MirroredGrouper {
	return &explicitMirroredGrouper{
		logger:              logger,
		instanceIDToGroupID: instanceToGroupID,
	}
}

// GroupAddingInstances places the given instances into groups according to their group ID.
// If there are more than p.ReplicaFactor() instances in a given group, the additional instances
// will be unused (*which* instances are unused is undefined, but this is logged, and
// can be ascertained by comparing the resulting placement to the list of added instances).
func (e *explicitMirroredGrouper) GroupAddingInstances(
	candidates []placement.Instance,
	p placement.Placement) ([][]placement.Instance, error) {
	return e.groupWithRF(candidates, p.ReplicaFactor())
}

// GroupInitialInstances places the given instances into groups according to their group ID.
// See GroupAddingInstances for additional detail--the behavior is the same.
func (e *explicitMirroredGrouper) GroupInitialInstances(
	candidates []placement.Instance,
	rf int) ([][]placement.Instance, error) {
	return e.groupWithRF(candidates, rf)
}

// GroupInstancesWithReplacements attempts to find a replacement instance in the same group
// for each of the leavingInstances
func (e *explicitMirroredGrouper) GroupInstancesWithReplacements(
	candidates []placement.Instance,
	leavingInstances []placement.Instance,
	p placement.Placement) ([]placement.MirroredReplacementGroup, error) {
	// find a replacement for each leaving instance.
	candidatesByGroup, err := e.groupInstancesByID(candidates)
	if err != nil {
		return nil, err
	}

	replacementGroups := make([]placement.MirroredReplacementGroup, 0, len(leavingInstances))
	for _, leavingInstance := range leavingInstances {
		// try to find an instance in the same group as the leaving instance.
		groupID, err := e.getGroup(leavingInstance)
		if err != nil {
			return nil, err
		}

		replacementGroup, ok := candidatesByGroup[groupID]
		// technically, a redundant check, but explicit is good.
		if !ok || len(replacementGroup) == 0 {
			return nil, fmt.Errorf(
				"leaving instance %s has no valid replacements in the same group (%s)",
				leavingInstance.ID(),
				groupID,
			)
		}

		// pop a replacement off and use it.
		replacementNode := replacementGroup[len(replacementGroup)-1]
		candidatesByGroup[groupID] = replacementGroup[:len(replacementGroup)-1]
		// ... fill in
		//
		replacementGroups = append(replacementGroups, placement.MirroredReplacementGroup{
			Leaving:     leavingInstance,
			Replacement: replacementNode})
	}
	return replacementGroups, nil
}

func (e *explicitMirroredGrouper) groupWithRF(
	candidates []placement.Instance,
	rf int) ([][]placement.Instance, error) {
	byGroupID, err := e.groupInstancesByID(candidates)
	if err != nil {
		return nil, err
	}

	groups := make([][]placement.Instance, 0, len(byGroupID))
	// validate and convert to slice
	for groupID, group := range byGroupID {
		if len(group) > rf {
			fullGroup := group
			group = group[:rf]

			var droppedIDs []string
			for _, dropped := range fullGroup[rf:] {
				droppedIDs = append(droppedIDs, dropped.ID())
			}
			e.logger.Warn(
				"explicitMirroredGrouper: found more hosts than RF in group; "+
					"using only RF hosts",
				zap.Strings("droppedIDs", droppedIDs),
				zap.String("groupID", groupID),
			)
			// return nil, fmt.Errorf(
			// 	"group with ID = %s has more than %d nodes (%d)",
			// 	gid,
			// 	rf,
			// 	len(group))
		}
		groups = append(groups, group)
	}
	return groups, nil
}

func (e *explicitMirroredGrouper) groupInstancesByID(candidates []placement.Instance) (map[string][]placement.Instance, error) {
	byGroupID := make(map[string][]placement.Instance)
	for _, candidate := range candidates {
		groupID, err := e.getGroup(candidate)
		if err != nil {
			return nil, err
		}

		byGroupID[groupID] = append(byGroupID[groupID], candidate)
	}
	return byGroupID, nil
}

// small wrapper around e.instanceIDToGroupID providing context on error.
func (e *explicitMirroredGrouper) getGroup(inst placement.Instance) (string, error) {
	groupID, err := e.instanceIDToGroupID(inst)
	if err != nil {
		return "", errors.Wrapf(err, "finding group for %s", inst.ID())
	}
	return groupID, nil
}

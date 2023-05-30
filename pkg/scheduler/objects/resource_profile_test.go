package objects

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func TestResourceProfile_NodeStatistics(t *testing.T) {
	selector := map[string]string{"key": "val"}
	node1 := newNodeWithAttrs("node01", parseResources("100G", "20"), selector)
	node2 := newNodeWithAttrs("node02", parseResources("200G", "40"), selector)
	node3 := newNodeWithAttrs("node03", parseResources("200G", "30"), map[string]string{})

	node1.SetReady(true)
	node2.SetReady(true)
	node3.SetReady(true)

	resource := parseResources("1000G", "100")
	profile := NewResourceProfile(resource, selector)
	profile.AddNode(node1)
	profile.AddNode(node2)
	profile.AddNode(node3)

	assert.Equal(t, 2, profile.GetNodeCount())
	assert.Assert(t, resources.Equals(profile.GetNodeCapacityResource(), parseResources("300G", "60")))
	assert.Assert(t, resources.Equals(profile.GetNodeAvailableResource(), profile.GetNodeCapacityResource()))
	assert.Assert(t, resources.IsZero(profile.GetNodeOccupiedResource()))
	assert.Assert(t, resources.IsZero(profile.GetNodeAllocatedResource()))

	node1.AddAllocation(newAllocation("app01", "foo", node1.NodeID, "root", parseResources("20G", "5")))
	node2.AddAllocation(newAllocation("app01", "foo", node2.NodeID, "root", parseResources("60G", "20")))

	time.Sleep(100 * time.Millisecond)
	assert.Assert(t, resources.Equals(profile.GetNodeCapacityResource(), parseResources("300G", "60")))
	assert.Assert(t, resources.Equals(profile.GetNodeAvailableResource(), parseResources("220G", "35")))
	assert.Assert(t, resources.Equals(profile.GetNodeAllocatedResource(), parseResources("80G", "25")))

	profile.RemoveNode(node1.NodeID)
	time.Sleep(100 * time.Millisecond)
	assert.Assert(t, resources.Equals(profile.GetNodeCapacityResource(), parseResources("200G", "40")))
	assert.Assert(t, resources.Equals(profile.GetNodeAvailableResource(), parseResources("140G", "20")))
	assert.Assert(t, resources.Equals(profile.GetNodeAllocatedResource(), parseResources("60G", "20")))

	profile.StopListening()
}

func newNodeWithAttrs(nodeID string, total *resources.Resource, attributes map[string]string) *Node {
	node := newNodeRes(nodeID, total)
	node.attributes = attributes
	return node
}

func parseResources(memory, cpu string) *resources.Resource {
	return resources.NewResourceFromMap(parseResourcesToMap(memory, cpu))
}

func parseResourcesToMap(memory, cpu string) map[string]resources.Quantity {
	return map[string]resources.Quantity{
		"memory": mustParse(resources.ParseQuantity(memory)),
		"cpu":    mustParse(resources.ParseVCore(cpu)),
	}
}

func mustParse(qty resources.Quantity, err error) resources.Quantity {
	if err != nil {
		panic(err)
	}
	return qty
}

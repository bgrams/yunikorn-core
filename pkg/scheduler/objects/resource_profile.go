package objects

import (
	"sync"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
)

type ResourceProfile struct {
	Resources    *resources.Resource
	NodeSelector map[string]string

	// Access should be locked
	nodes                 map[string]*Node
	applications          map[string]*Application
	nodeAllocatedResource *resources.Resource
	nodeAvailableResource *resources.Resource
	nodeOccupiedResource  *resources.Resource
	nodeCapacityResource  *resources.Resource
	nodeUpdated           chan struct{}
	stopChan              chan struct{}
	sync.RWMutex
}

func NewResourceProfile(r *resources.Resource, s map[string]string) *ResourceProfile {
	profile := &ResourceProfile{
		Resources:    r,
		NodeSelector: s,
		nodes:        make(map[string]*Node),
		applications: make(map[string]*Application),
		nodeUpdated:  make(chan struct{}, 1),
		stopChan:     make(chan struct{}, 1),
	}
	profile.refreshNodeStatistics()
	profile.StartListening()
	return profile
}

func NewResourceProfileFromConfig(conf configs.ResourceProfileConfig) (*ResourceProfile, error) {
	r, err := resources.NewResourceFromConf(conf.Resources)
	if err != nil {
		return nil, err
	}
	return NewResourceProfile(r, conf.NodeSelectors), nil
}

func (rp *ResourceProfile) AddApplication(app *Application) {
	rp.Lock()
	defer rp.Unlock()

	rp.applications[app.ApplicationID] = app
}

func (rp *ResourceProfile) RemoveApplication(appID string) {
	rp.Lock()
	defer rp.Unlock()

	delete(rp.applications, appID)
}

func (rp *ResourceProfile) NodeUpdated(node *Node) {
	rp.Lock()
	defer rp.Unlock()

	if _, ok := rp.nodes[node.NodeID]; !ok {
		rp.nodes[node.NodeID] = node
	}
	rp.nodeUpdated <- struct{}{}
}

func (rp *ResourceProfile) AddNode(node *Node) {
	if rp.matchesNode(node) {
		rp.addNodeInternal(node)
		node.AddListener(rp)
	}
}

func (rp *ResourceProfile) RemoveNode(nodeID string) {
	rp.Lock()
	defer rp.Unlock()

	if node, ok := rp.nodes[nodeID]; ok {
		delete(rp.nodes, nodeID)
		rp.nodeAllocatedResource.SubFrom(node.allocatedResource)
		rp.nodeAvailableResource.SubFrom(node.availableResource)
		rp.nodeOccupiedResource.SubFrom(node.occupiedResource)
		rp.nodeCapacityResource.SubFrom(node.totalResource)
		node.RemoveListener(rp)
	}
}

func (rp *ResourceProfile) GetNodeAllocatedResource() *resources.Resource {
	rp.RLock()
	defer rp.RUnlock()
	return rp.nodeAllocatedResource.Clone()
}

func (rp *ResourceProfile) GetNodeAvailableResource() *resources.Resource {
	rp.RLock()
	defer rp.RUnlock()
	return rp.nodeAvailableResource.Clone()
}

func (rp *ResourceProfile) GetNodeOccupiedResource() *resources.Resource {
	rp.RLock()
	defer rp.RUnlock()
	return rp.nodeOccupiedResource.Clone()
}

func (rp *ResourceProfile) GetNodeCapacityResource() *resources.Resource {
	rp.RLock()
	defer rp.RUnlock()
	return rp.nodeCapacityResource.Clone()
}

func (rp *ResourceProfile) GetNodeCount() int {
	rp.RLock()
	defer rp.RUnlock()
	return len(rp.nodes)
}

func (rp *ResourceProfile) StartListening() {
	rp.listenForUpdates()
}

func (rp *ResourceProfile) StopListening() {
	rp.stopChan <- struct{}{}
}

func (rp *ResourceProfile) refreshNodeStatistics() {
	rp.Lock()
	defer rp.Unlock()

	rp.nodeAllocatedResource = resources.NewResource()
	rp.nodeAvailableResource = resources.NewResource()
	rp.nodeOccupiedResource = resources.NewResource()
	rp.nodeCapacityResource = resources.NewResource()

	for _, node := range rp.nodes {
		if node.IsReady() {
			rp.nodeAllocatedResource.AddTo(node.GetAllocatedResource())
			rp.nodeAvailableResource.AddTo(node.GetAvailableResource())
			rp.nodeCapacityResource.AddTo(node.GetCapacity())
			rp.nodeOccupiedResource.AddTo(node.GetOccupiedResource())
		}
	}
}

func (rp *ResourceProfile) listenForUpdates() {
	go func() {
		for {
			select {
			case <-rp.nodeUpdated:
				rp.refreshNodeStatistics()
			case <-rp.stopChan:
				return
			}
		}
	}()
}

func (rp *ResourceProfile) addNodeInternal(node *Node) {
	rp.Lock()
	defer rp.Unlock()

	rp.nodes[node.NodeID] = node
	rp.nodeAllocatedResource.AddTo(node.allocatedResource)
	rp.nodeAvailableResource.AddTo(node.availableResource)
	rp.nodeOccupiedResource.AddTo(node.occupiedResource)
	rp.nodeCapacityResource.AddTo(node.totalResource)
}

func (rp *ResourceProfile) matchesNode(node *Node) bool {
	if rp.NodeSelector == nil {
		return true
	}

	for k, v := range rp.NodeSelector {
		if node.attributes != nil && node.GetAttribute(k) != v {
			return false
		}
	}
	return true
}

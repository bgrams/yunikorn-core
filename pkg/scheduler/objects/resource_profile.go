package objects

import (
	"sync"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler"
)

type ResourceProfile struct {
	sync.RWMutex
	partition    *scheduler.PartitionContext
	Resources    *resources.Resource
	NodeSelector map[string]string

	// All app access should be locked
	applications map[string]*Application
}

func (rp *ResourceProfile) AddApplication(app *Application) {
	rp.Lock()
	defer rp.Unlock()

	rp.applications[app.ApplicationID] = app
}

func (rp *ResourceProfile) RemoveApplication(app *Application) {
	rp.Lock()
	defer rp.Unlock()

	delete(rp.applications, app.ApplicationID)
}

func (rp *ResourceProfile) GetPendingAppResource() *resources.Resource {
	rp.RLock()
	defer rp.RUnlock()

	resource := resources.NewResource()
	for _, app := range rp.applications {
		app.GetPendingResource().AddTo(resource)
	}
	return resource
}

func (rp *ResourceProfile) GetOccupiedNodeResource() *resources.Resource {
	resource := resources.NewResource()
	for _, node := range rp.partition.GetNodes() {
		if !rp.matchesNode(node) {
			continue
		}
		node.GetOccupiedResource().AddTo(resource)
	}
	return resource
}

func (rp *ResourceProfile) matchesNode(node *Node) bool {
	for k, v := range rp.NodeSelector {
		if node.GetAttribute(k) != v {
			return false
		}
	}
	return true
}

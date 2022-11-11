/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package capacity

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"
	"math"
	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/helpers"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName        = "capacity"
	GuaranteeCapacity = "capacity.GuaranteeEnable"
	ElasticCapacity   = "capacity.ElasticEnable"
)

type capacityPlugin struct {
	totalResource *api.Resource
	queueOpts     map[api.QueueID]*queueAttr
	// Arguments given for the plugin
	pluginArguments framework.Arguments
	capacityEnable  *capacityEnable
}

type queueAttr struct {
	queueID api.QueueID
	name    string
	weight  int32
	share   float64

	deserved  *api.Resource
	allocated *api.Resource
	request   *api.Resource
	// elastic represents the sum of job's elastic resource, job's elastic = job.allocated - job.minAvailable
	elastic *api.Resource
	// inqueue represents the resource request of the inqueue job
	inqueue    *api.Resource
	capability *api.Resource
	// realCapability represents the resource limit of the queue, LessEqual capability
	realCapability *api.Resource
	guarantee      *api.Resource
}

type capacityEnable struct {
	elasticEnable   bool
	guaranteeEnable bool
	totalGuarantee  *api.Resource
}

func enableCapacity(args framework.Arguments, ssn *framework.Session) *capacityEnable {
	capacityEnable := &capacityEnable{
		guaranteeEnable: true,
		elasticEnable:   true,
		totalGuarantee:  api.EmptyResource(),
	}
	args.GetBool(&capacityEnable.guaranteeEnable, GuaranteeCapacity)
	args.GetBool(&capacityEnable.elasticEnable, ElasticCapacity)
	klog.V(4).Infof("Enable guarantee in capacity plugin: <%v>", capacityEnable.guaranteeEnable)
	if capacityEnable.guaranteeEnable {
		for _, queue := range ssn.Queues {
			if len(queue.Queue.Spec.Guarantee.Resource) == 0 {
				continue
			}
			guaranteeTmp := api.NewResource(queue.Queue.Spec.Guarantee.Resource)
			capacityEnable.totalGuarantee.Add(guaranteeTmp)
		}
		klog.V(4).Infof("The total capacityEnable resource is <%v>", capacityEnable.totalGuarantee)
	}
	return capacityEnable
}

// New return proportion action
func New(arguments framework.Arguments) framework.Plugin {
	return &capacityPlugin{
		totalResource:   api.EmptyResource(),
		queueOpts:       map[api.QueueID]*queueAttr{},
		pluginArguments: arguments,
		capacityEnable:  &capacityEnable{},
	}
}

func (pp *capacityPlugin) Name() string {
	return PluginName
}

func (pp *capacityPlugin) OnSessionOpen(ssn *framework.Session) {
	// Prepare scheduling data for this session.
	pp.totalResource.Add(ssn.TotalResource)

	klog.V(4).Infof("The total resource is <%v>", pp.totalResource)

	pp.capacityEnable = enableCapacity(pp.pluginArguments, ssn)

	// Build attributes for Queues.
	for _, job := range ssn.Jobs {
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		if _, found := pp.queueOpts[job.Queue]; !found {
			pp.generateQueueCommonInfo(ssn, job)
		}

		attr := pp.queueOpts[job.Queue]
		for status, tasks := range job.TaskStatusIndex {
			if api.AllocatedStatus(status) {
				for _, t := range tasks {
					attr.allocated.Add(t.Resreq)
					attr.request.Add(t.Resreq)
				}
			} else if status == api.Pending {
				for _, t := range tasks {
					attr.request.Add(t.Resreq)
				}
			}
		}

		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue {
			attr.inqueue.Add(job.GetMinResources())
		}

		// calculate inqueue resource for running jobs
		// the judgement 'job.PodGroup.Status.Running >= job.PodGroup.Spec.MinMember' will work on cases such as the following condition:
		// Considering a Spark job is completed(driver pod is completed) while the podgroup keeps running, the allocated resource will be reserved again if without the judgement.
		if job.PodGroup.Status.Phase == scheduling.PodGroupRunning &&
			job.PodGroup.Spec.MinResources != nil &&
			int32(util.CalculateAllocatedTaskNum(job)) >= job.PodGroup.Spec.MinMember {
			allocated := util.GetAllocatedResource(job)
			inqueued := util.GetInqueueResource(job, allocated)
			attr.inqueue.Add(inqueued)
		}

		// only calculate elastic resource when elastic enable
		if pp.capacityEnable.elasticEnable {
			attr.elastic.Add(job.GetElasticResources())
		}
	}

	// Record metrics
	for queueID, queueInfo := range ssn.Queues {
		if _, ok := pp.queueOpts[queueID]; !ok {
			metrics.UpdateQueueAllocatedMetrics(queueInfo.Name, &api.Resource{}, &api.Resource{}, &api.Resource{}, &api.Resource{})
		} else {
			attr := pp.queueOpts[queueID]
			metrics.UpdateQueueAllocatedMetrics(attr.name, attr.allocated, attr.capability, attr.guarantee, attr.realCapability)
			metrics.UpdateQueueRequest(attr.name, attr.request.MilliCPU, attr.request.Memory)
			metrics.UpdateQueueWeight(attr.name, attr.weight)
			queue := ssn.Queues[attr.queueID]
			metrics.UpdateQueuePodGroupStatusCount(attr.name, queue.Queue.Status)
		}
	}

	// generate deserved resource for queue
	for _, attr := range pp.queueOpts {
		attr.deserved.MinDimensionResource(attr.realCapability, api.Infinity)
		attr.deserved.MinDimensionResource(attr.request, api.Zero)
		if pp.capacityEnable.guaranteeEnable {
			attr.deserved = helpers.Max(attr.deserved, attr.guarantee)
		}
		pp.updateShare(attr)
		klog.V(5).Infof("Queue %s capacity <%s> realCapacity <%s> allocated <%s> request <%s> deserved <%s> inqueue <%s> elastic <%s>",
			attr.name, attr.capability.String(), attr.realCapability.String(), attr.allocated.String(), attr.request.String(),
			attr.deserved.String(), attr.inqueue.String(), attr.elastic.String())
	}

	ssn.AddQueueOrderFn(pp.Name(), func(l, r interface{}) int {
		lv := l.(*api.QueueInfo)
		rv := r.(*api.QueueInfo)

		if pp.queueOpts[lv.UID].share == pp.queueOpts[rv.UID].share {
			return 0
		}

		if pp.queueOpts[lv.UID].share < pp.queueOpts[rv.UID].share {
			return -1
		}

		return 1
	})

	ssn.AddReclaimableFn(pp.Name(), func(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		var victims []*api.TaskInfo
		allocations := map[api.QueueID]*api.Resource{}

		for _, reclaimee := range reclaimees {
			job := ssn.Jobs[reclaimee.Job]
			attr := pp.queueOpts[job.Queue]

			if _, found := allocations[job.Queue]; !found {
				allocations[job.Queue] = attr.allocated.Clone()
			}
			allocated := allocations[job.Queue]
			if allocated.LessPartly(reclaimer.Resreq, api.Zero) {
				klog.V(3).Infof("Failed to allocate resource for Task <%s/%s> in Queue <%s>, not enough resource.",
					reclaimee.Namespace, reclaimee.Name, job.Queue)
				continue
			}

			if !allocated.LessEqual(attr.deserved, api.Zero) {
				allocated.Sub(reclaimee.Resreq)
				victims = append(victims, reclaimee)
			}
		}
		klog.V(4).Infof("Victims from capacity plugins are %+v", victims)
		return victims, util.Permit
	})

	ssn.AddOverusedFn(pp.Name(), func(obj interface{}) bool {
		queue := obj.(*api.QueueInfo)
		attr := pp.queueOpts[queue.UID]

		overused := attr.deserved.LessEqual(attr.allocated, api.Zero)
		metrics.UpdateQueueOverused(attr.name, overused)
		if overused {
			klog.V(3).Infof("Queue <%v>: deserved <%v>, allocated <%v>, share <%v>",
				queue.Name, attr.deserved, attr.allocated, attr.share)
		}

		return overused
	})

	ssn.AddAllocatableFn(pp.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		attr := pp.queueOpts[queue.UID]

		free, _ := attr.deserved.Diff(attr.allocated, api.Zero)
		allocatable := candidate.Resreq.LessEqual(free, api.Zero)
		if !allocatable {
			klog.V(3).Infof("Queue <%v>: deserved <%v>, allocated <%v>; Candidate <%v>: resource request <%v>",
				queue.Name, attr.deserved, attr.allocated, candidate.Name, candidate.Resreq)
		}

		return allocatable
	})

	ssn.AddJobEnqueueableFn(pp.Name(), func(obj interface{}) int {
		job := obj.(*api.JobInfo)
		queueID := job.Queue
		attr := pp.queueOpts[queueID]
		queue := ssn.Queues[queueID]
		// If no capability is set, always enqueue the job.
		if attr.realCapability == nil {
			klog.V(4).Infof("Capability of queue <%s> was not set, allow job <%s/%s> to Inqueue.",
				queue.Name, job.Namespace, job.Name)
			return util.Permit
		}

		if job.PodGroup.Spec.MinResources == nil {
			klog.V(4).Infof("job %s MinResources is null, allow it to Inqueue.", job.Name)
			return util.Permit
		}
		minReq := job.GetMinResources()

		klog.V(5).Infof("job %s min resource <%s>, queue %s capacity <%s> realCapability <%s> allocated <%s> inqueue <%s> elastic <%s>",
			job.Name, minReq.String(), queue.Name, attr.capability.String(), attr.realCapability.String(), attr.allocated.String(), attr.inqueue.String(), attr.elastic.String())
		// The queue resource quota limit has not reached
		totalNeedResource := minReq.Add(attr.allocated).Add(attr.inqueue)
		inqueue := false
		if pp.capacityEnable.elasticEnable {
			inqueue = totalNeedResource.Sub(attr.elastic).LessEqual(attr.realCapability, api.Infinity)
		} else {
			inqueue = totalNeedResource.LessEqual(attr.realCapability, api.Infinity)
		}
		klog.V(5).Infof("job %s inqueue %v", job.Name, inqueue)
		if inqueue {
			attr.inqueue.Add(job.GetMinResources())
			return util.Permit
		}
		ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, string(scheduling.PodGroupUnschedulableType), "queue resource quota insufficient")
		return util.Reject
	})

	// Register event handlers.
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			job := ssn.Jobs[event.Task.Job]
			attr := pp.queueOpts[job.Queue]
			attr.allocated.Add(event.Task.Resreq)
			metrics.UpdateQueueAllocatedMetrics(attr.name, attr.allocated, attr.capability, attr.guarantee, attr.realCapability)

			pp.updateShare(attr)

			klog.V(4).Infof("Capacity AllocateFunc: task <%v/%v>, resreq <%v>,  share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share)
		},
		DeallocateFunc: func(event *framework.Event) {
			job := ssn.Jobs[event.Task.Job]
			attr := pp.queueOpts[job.Queue]
			attr.allocated.Sub(event.Task.Resreq)
			metrics.UpdateQueueAllocatedMetrics(attr.name, attr.allocated, attr.capability, attr.guarantee, attr.realCapability)

			pp.updateShare(attr)

			klog.V(4).Infof("Capacity EvictFunc: task <%v/%v>, resreq <%v>,  share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share)
		},
	})
}

func (pp *capacityPlugin) generateQueueCommonInfo(ssn *framework.Session, job *api.JobInfo) {
	queue := ssn.Queues[job.Queue]
	attr := &queueAttr{
		queueID: queue.UID,
		name:    queue.Name,

		deserved:  api.EmptyResource(),
		allocated: api.EmptyResource(),
		request:   api.EmptyResource(),
		elastic:   api.EmptyResource(),
		inqueue:   api.EmptyResource(),
		guarantee: api.EmptyResource(),
	}
	if len(queue.Queue.Spec.Capability) != 0 {
		attr.capability = api.NewResource(queue.Queue.Spec.Capability)
		if attr.capability.MilliCPU <= 0 {
			attr.capability.MilliCPU = math.MaxFloat64
		}
		if attr.capability.Memory <= 0 {
			attr.capability.Memory = math.MaxFloat64
		}
	}
	realCapability := pp.totalResource.Clone()
	if pp.capacityEnable.guaranteeEnable {
		if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
			attr.guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
		}
		realCapability = realCapability.Sub(pp.capacityEnable.totalGuarantee).Add(attr.guarantee)
	}
	if attr.capability == nil {
		attr.realCapability = realCapability
	} else {
		attr.realCapability = helpers.Min(realCapability, attr.capability)
	}
	pp.queueOpts[job.Queue] = attr
	klog.V(4).Infof("Added Queue <%s> attributes.", job.Queue)
}

func (pp *capacityPlugin) OnSessionClose(ssn *framework.Session) {
	pp.totalResource = nil
	pp.queueOpts = nil
	pp.capacityEnable = nil
}

func (pp *capacityPlugin) updateShare(attr *queueAttr) {
	res := float64(0)

	// TODO(k82cn): how to handle fragment issues?
	for _, rn := range attr.deserved.ResourceNames() {
		share := helpers.Share(attr.allocated.Get(rn), attr.deserved.Get(rn))
		if share > res {
			res = share
		}
	}

	attr.share = res
	metrics.UpdateQueueShare(attr.name, attr.share)
}

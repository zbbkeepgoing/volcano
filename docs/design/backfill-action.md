# Backfill

## Introduction

In Volcano there are 6 actions such as `enqueue`, `allocate`, `preempt`, `reclaim`, `backfill`, `shuffle` and with the help of
plugins like `conformance`, `drf`, `gang`, `nodeorder` and more plugins. All these plugins provides
behavioural characteristics how scheduler make scheduling decisions.

## Backfill Action

Backfill is one of the actions in Volcano scheduler.  Backfill action comes into play when the pod that does not 
specify the resource request. When executing the scheduling action on a single pod, backfill will traverse all nodes 
and schedule the pod to this node as long as the node meets the scheduling request of pod.

In a cluster, the main resources are occupied by `fat jobs`, backFill actions allow the cluster to quickly schedule “small jobs”.
Improve cluster throughput and resource utilization.

In Backfill Action, there are multiple plugin functions that are getting used like,

1. PrePredicateFn(Plugin: Predicates),
2. PredicateFn(Plugin: Predicates),

### 1. PrePredicateFn:
#### Predicates:
PrePredicateFn returns whether a task can be bounded to some node or not by running through set of predicates.
Mainly the `preFilter` func of NodePorts, InterPodAffinity and PodTopologySpread plugin in the k8s native scheduler framework takes effect.

### 2. PredicateFn:
#### Predicates:
PredicateFn returns whether a task can be bounded to a node or not by running through set of predicates.
Mainly the `filter` func of NodeUnschedulable, NodeAffinity, TaintToleration, NodePorts, InterPodAffinity, CSILimits, VolumeZone 
and PodTopologySpread plugin in the k8s native scheduler framework takes effect.

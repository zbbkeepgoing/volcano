/*
Copyright 2019 The Volcano Authors.

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

package podgroup

import (
	"context"
	"encoding/json"
	"strings"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog"
	quotacore "k8s.io/kubernetes/pkg/quota/v1/evaluator/core"
	"k8s.io/utils/clock"

	"volcano.sh/apis/pkg/apis/helpers"
	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

type podRequest struct {
	podName      string
	podNamespace string
}

func (pg *pgcontroller) addPod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.Errorf("Failed to convert %v to v1.Pod", obj)
		return
	}

	req := podRequest{
		podName:      pod.Name,
		podNamespace: pod.Namespace,
	}

	pg.queue.Add(req)
}

func (pg *pgcontroller) updatePodAnnotations(pod *v1.Pod, pgName string) error {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	if pod.Annotations[scheduling.KubeGroupNameAnnotationKey] == "" {
		pod.Annotations[scheduling.KubeGroupNameAnnotationKey] = pgName
	} else {
		if pod.Annotations[scheduling.KubeGroupNameAnnotationKey] != pgName {
			klog.Errorf("normal pod %s/%s annotations %s value is not %s, but %s", pod.Namespace, pod.Name,
				scheduling.KubeGroupNameAnnotationKey, pgName, pod.Annotations[scheduling.KubeGroupNameAnnotationKey])
		}
		return nil
	}

	if _, err := pg.kubeClient.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("Failed to update pod <%s/%s>: %v", pod.Namespace, pod.Name, err)
		return err
	}

	return nil
}

func (pg *pgcontroller) getAnnotationsFromUpperRes(kind string, name string, namespace string) map[string]string {
	switch kind {
	case "ReplicaSet":
		rs, err := pg.kubeClient.AppsV1().ReplicaSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to get upper %s for Pod <%s/%s>: %v", kind, namespace, name, err)
			return map[string]string{}
		}
		return rs.Annotations
	case "DaemonSet":
		ds, err := pg.kubeClient.AppsV1().DaemonSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to get upper %s for Pod <%s/%s>: %v", kind, namespace, name, err)
			return map[string]string{}
		}
		return ds.Annotations
	case "StatefulSet":
		ss, err := pg.kubeClient.AppsV1().StatefulSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to get upper %s for Pod <%s/%s>: %v", kind, namespace, name, err)
			return map[string]string{}
		}
		return ss.Annotations
	case "Job":
		job, err := pg.kubeClient.BatchV1().Jobs(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to get upper %s for Pod <%s/%s>: %v", kind, namespace, name, err)
			return map[string]string{}
		}
		return job.Annotations
	default:
		return map[string]string{}
	}
}

func (pg *pgcontroller) createNormalPodPGIfNotExist(pod *v1.Pod) error {
	var resourceList *v1.ResourceList
	pgName := helpers.GeneratePodgroupName(pod)
	if pod.Annotations != nil && pod.Annotations[scheduling.KubeGroupNameAnnotationKey] != "" {
		pgName = pod.Annotations[scheduling.KubeGroupNameAnnotationKey]
		if pod.Annotations[scheduling.VolcanoGroupMinResourcesAnnotationKey] != "" {
			minResources := pod.Annotations[scheduling.VolcanoGroupMinResourcesAnnotationKey]
			err := json.Unmarshal([]byte(minResources), &resourceList)
			if err != nil {
				return err
			}
		}
	}
	var errGetPG error
	var podgroup *scheduling.PodGroup
	if podgroup, errGetPG = pg.pgLister.PodGroups(pod.Namespace).Get(pgName); errGetPG == nil {
		klog.V(5).Infof("pod %v/%v has created podgroup", pod.Namespace, pod.Name)
		_, isDependent := isDependentPod(pod)
		if !isDependent {
			err := pg.updatePGOwnerReference(pod, podgroup)
			if err != nil {
				return err
			}
		}
		return nil
	}

	if !apierrors.IsNotFound(errGetPG) {
		klog.Errorf("Failed to get normal PodGroup for Pod <%s/%s>: %v",
			pod.Namespace, pod.Name, errGetPG)
		return errGetPG
	}

	if resourceList == nil {
		resourceList = calcPGMinResources(pod)
	}

	obj := &scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       pod.Namespace,
			Name:            pgName,
			OwnerReferences: newPGOwnerReferences(pod),
			Annotations:     map[string]string{},
			Labels:          map[string]string{},
		},
		Spec: scheduling.PodGroupSpec{
			MinMember:         1,
			PriorityClassName: pod.Spec.PriorityClassName,
			MinResources:      resourceList,
		},
		Status: scheduling.PodGroupStatus{
			Phase: scheduling.PodGroupPending,
		},
	}

	// Inherit annotations from upper resources.
	for _, reference := range pod.OwnerReferences {
		if reference.Kind != "" && reference.Name != "" {
			var upperAnnotations = pg.getAnnotationsFromUpperRes(reference.Kind, reference.Name, pod.Namespace)
			for k, v := range upperAnnotations {
				if strings.HasPrefix(k, scheduling.AnnotationPrefix) {
					obj.Annotations[k] = v
				}
			}
		}
	}

	// Individual annotations on pods would overwrite annotations inherited from upper resources.
	if queueName, ok := pod.Annotations[scheduling.QueueNameAnnotationKey]; ok {
		obj.Spec.Queue = queueName
	}

	if value, ok := pod.Annotations[scheduling.PodPreemptable]; ok {
		obj.Annotations[scheduling.PodPreemptable] = value
	}
	if value, ok := pod.Annotations[scheduling.CooldownTime]; ok {
		obj.Annotations[scheduling.CooldownTime] = value
	}
	if value, ok := pod.Annotations[scheduling.RevocableZone]; ok {
		obj.Annotations[scheduling.RevocableZone] = value
	}
	if value, ok := pod.Labels[scheduling.PodPreemptable]; ok {
		obj.Labels[scheduling.PodPreemptable] = value
	}
	if value, ok := pod.Labels[scheduling.CooldownTime]; ok {
		obj.Labels[scheduling.CooldownTime] = value
	}

	if value, found := pod.Annotations[scheduling.JDBMinAvailable]; found {
		obj.Annotations[scheduling.JDBMinAvailable] = value
	} else if value, found := pod.Annotations[scheduling.JDBMaxUnavailable]; found {
		obj.Annotations[scheduling.JDBMaxUnavailable] = value
	}

	if _, err := pg.vcClient.SchedulingV1beta1().PodGroups(pod.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{}); err != nil {
		klog.Errorf("Failed to create normal PodGroup for Pod <%s/%s>: %v",
			pod.Namespace, pod.Name, err)
		return err
	}

	return pg.updatePodAnnotations(pod, pgName)
}

// updatePGOwnerReference update podgroup's owner reference when pod is a dependent
func (pg *pgcontroller) updatePGOwnerReference(pod *v1.Pod, podgroup *scheduling.PodGroup) error {
	for _, value := range podgroup.OwnerReferences {
		if pod.UID == value.UID {
			return nil
		}
	}

	controller := false
	newRef := metav1.NewControllerRef(pod, schema.GroupVersionKind{
		Group:   v1.SchemeGroupVersion.Group,
		Version: v1.SchemeGroupVersion.Version,
		Kind:    "Pod",
	})
	newRef.Controller = &controller
	podgroup.SetOwnerReferences(append(podgroup.OwnerReferences, *newRef))
	if _, err := pg.vcClient.SchedulingV1beta1().PodGroups(pod.Namespace).Update(context.TODO(), podgroup, metav1.UpdateOptions{}); err != nil {
		klog.Errorf("Failed to update normal PodGroup for Pod <%s/%s>: %v",
			pod.Namespace, pod.Name, err)
		return err
	}
	return nil
}

func newPGOwnerReferences(pod *v1.Pod) []metav1.OwnerReference {
	podReferences, isDependent := isDependentPod(pod)
	if isDependent {
		return podReferences
	}

	gvk := schema.GroupVersionKind{
		Group:   v1.SchemeGroupVersion.Group,
		Version: v1.SchemeGroupVersion.Version,
		Kind:    "Pod",
	}
	ref := metav1.NewControllerRef(pod, gvk)
	return []metav1.OwnerReference{*ref}
}

// isDependentPod check pod is a dependent or not
func isDependentPod(pod *v1.Pod) ([]metav1.OwnerReference, bool) {
	if len(pod.OwnerReferences) != 0 {
		for _, ownerReference := range pod.OwnerReferences {
			if ownerReference.Controller != nil && *ownerReference.Controller {
				return pod.OwnerReferences, true
			}
		}
	}
	return nil, false
}

// calcPGMinResources calculate podgroup minimum resource
func calcPGMinResources(pod *v1.Pod) *v1.ResourceList {
	pgMinRes, _ := quotacore.PodUsageFunc(pod, clock.RealClock{})

	return &pgMinRes
}

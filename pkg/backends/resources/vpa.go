/*
Copyright (C) GRyCAP - I3M - UPV

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

package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/grycap/oscar/v4/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	vpaNameSuffix    = "-vpa"
	vpaUpdateModeOff = "Off"
)

var vpaGVR = schema.GroupVersionResource{
	Group:    "autoscaling.k8s.io",
	Version:  "v1",
	Resource: "verticalpodautoscalers",
}

var vpaCheckpointGVR = schema.GroupVersionResource{
	Group:    "autoscaling.k8s.io",
	Version:  "v1",
	Resource: "verticalpodautoscalercheckpoints",
}

func getVPAName(serviceName string) string {
	return serviceName + vpaNameSuffix
}

// Checkpoint names follow "<vpa-name>-<container-name>" (confirmed experimentally in VPA-002,
// docs/tfm/experiments/002-vpa-nginx-expose/results.md); Expose services always run a single
// container (types.ContainerName), so no listing/label-selector lookup is needed.
func getVPACheckpointName(vpaName string) string {
	return vpaName + "-" + types.ContainerName
}

func getVPASpec(service types.Service, namespace string, cfg *types.Config) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "autoscaling.k8s.io/v1",
		"kind":       "VerticalPodAutoscaler",
		"metadata": map[string]any{
			"name":      getVPAName(service.Name),
			"namespace": namespace,
		},
		"spec": map[string]any{
			"targetRef": map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"name":       GetDeploymentName(service.Name),
			},
			"updatePolicy": map[string]any{
				"updateMode": vpaUpdateModeOff,
			},
		},
	}}
}

// Mirrors isMissingKueueAPI (pkg/handlers/quotas.go) and isMissingKedaAPI (keda.go).
func isMissingVpaAPI(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "the server could not find the requested resource") ||
		strings.Contains(msg, "no matches for kind")
}

func wrapMissingVpaAPIError(err error) error {
	if err == nil {
		return nil
	}
	if isMissingVpaAPI(err) {
		return fmt.Errorf("vpa VerticalPodAutoscaler CRD not available in the cluster: %w", err)
	}
	return err
}

func createVPA(service types.Service, namespace string, cfg *types.Config) error {
	gatewayClientset, err := gatewayClientsetProvider()
	if err != nil {
		return err
	}

	vpa := getVPASpec(service, namespace, cfg)
	_, err = gatewayClientset.Resource(vpaGVR).Namespace(namespace).Create(context.TODO(), vpa, metav1.CreateOptions{})
	return wrapMissingVpaAPIError(err)
}

func updateVPA(service types.Service, namespace string, cfg *types.Config) error {
	gatewayClientset, err := gatewayClientsetProvider()
	if err != nil {
		return err
	}

	vpa := getVPASpec(service, namespace, cfg)
	current, err := gatewayClientset.Resource(vpaGVR).Namespace(namespace).Get(context.TODO(), vpa.GetName(), metav1.GetOptions{})
	if err != nil {
		return wrapMissingVpaAPIError(err)
	}
	vpa.SetResourceVersion(current.GetResourceVersion())

	_, err = gatewayClientset.Resource(vpaGVR).Namespace(namespace).Update(context.TODO(), vpa, metav1.UpdateOptions{})
	return wrapMissingVpaAPIError(err)
}

func deleteVPACheckpoint(vpaName string, namespace string) error {
	gatewayClientset, err := gatewayClientsetProvider()
	if err != nil {
		return err
	}

	err = gatewayClientset.Resource(vpaCheckpointGVR).Namespace(namespace).Delete(context.TODO(), getVPACheckpointName(vpaName), metav1.DeleteOptions{})
	return ignoreNotFound(err)
}

// VPA 1.7.0 does not garbage-collect its checkpoint (no ownerReferences, see
// docs/tfm/experiments/002-vpa-nginx-expose/results.md), so it's deleted explicitly here.
func deleteVPA(name string, namespace string) error {
	gatewayClientset, err := gatewayClientsetProvider()
	if err != nil {
		return err
	}

	err = gatewayClientset.Resource(vpaGVR).Namespace(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err = ignoreNotFound(err); err != nil {
		return err
	}

	return deleteVPACheckpoint(name, namespace)
}

func existsVPA(serviceName string, namespace string) bool {
	gatewayClientset, err := gatewayClientsetProvider()
	if err != nil {
		return false
	}

	_, err = gatewayClientset.Resource(vpaGVR).Namespace(namespace).Get(context.TODO(), getVPAName(serviceName), metav1.GetOptions{})
	return err == nil
}

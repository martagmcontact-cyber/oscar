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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/grycap/oscar/v4/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Lets pkg/handlers distinguish this client input error (400) from a Kubernetes/RBAC failure (500).
var ErrInvalidKedaConfig = errors.New("invalid keda configuration")

const (
	kedaScaledObjectSuffix = "-scaledobject"
	kedaCronTriggerType    = "cron"
	kedaDefaultTimezone    = "Etc/UTC"
)

var scaledObjectGVR = schema.GroupVersionResource{
	Group:    "keda.sh",
	Version:  "v1alpha1",
	Resource: "scaledobjects",
}

func getScaledObjectName(serviceName string) string {
	return serviceName + kedaScaledObjectSuffix
}

// Does not parse the cron expressions themselves (KEDA's own webhook does), nor compare
// DesiredReplicas to MaxScale (no such validation exists for the native HPA either).
func validateKedaConfig(service types.Service) error {
	cron := service.Expose.Keda.Cron

	if strings.TrimSpace(cron.Start) == "" {
		return fmt.Errorf("%w for service %q: cron 'start' must not be empty", ErrInvalidKedaConfig, service.Name)
	}
	if strings.TrimSpace(cron.End) == "" {
		return fmt.Errorf("%w for service %q: cron 'end' must not be empty", ErrInvalidKedaConfig, service.Name)
	}
	if strings.TrimSpace(cron.DesiredReplicas) == "" {
		return fmt.Errorf("%w for service %q: cron 'desired_replicas' must not be empty", ErrInvalidKedaConfig, service.Name)
	}

	desiredReplicas, err := strconv.Atoi(strings.TrimSpace(cron.DesiredReplicas))
	if err != nil {
		return fmt.Errorf("%w for service %q: cron 'desired_replicas' must be an integer: %v", ErrInvalidKedaConfig, service.Name, err)
	}
	if desiredReplicas < 0 {
		return fmt.Errorf("%w for service %q: cron 'desired_replicas' must be a non-negative integer", ErrInvalidKedaConfig, service.Name)
	}

	return nil
}

// Omits pollingInterval/cooldownPeriod, letting KEDA apply its own defaults.
func getScaledObjectSpec(service types.Service, namespace string, cfg *types.Config) *unstructured.Unstructured {
	timezone := strings.TrimSpace(service.Expose.Keda.Cron.Timezone)
	if timezone == "" {
		timezone = kedaDefaultTimezone
	}

	trigger := map[string]any{
		"type": kedaCronTriggerType,
		"metadata": map[string]any{
			"timezone":        timezone,
			"start":           service.Expose.Keda.Cron.Start,
			"end":             service.Expose.Keda.Cron.End,
			"desiredReplicas": service.Expose.Keda.Cron.DesiredReplicas,
		},
	}

	spec := map[string]any{
		"scaleTargetRef": map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"name":       GetDeploymentName(service.Name),
		},
		"minReplicaCount": int64(service.Expose.MinScale),
		"maxReplicaCount": int64(service.Expose.MaxScale),
		"triggers":        []any{trigger},
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "keda.sh/v1alpha1",
		"kind":       "ScaledObject",
		"metadata": map[string]any{
			"name":      getScaledObjectName(service.Name),
			"namespace": namespace,
		},
		"spec": spec,
	}}
}

// Mirrors isMissingKueueAPI (pkg/handlers/quotas.go) for the same purpose.
func isMissingKedaAPI(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "the server could not find the requested resource") ||
		strings.Contains(msg, "no matches for kind")
}

func wrapMissingKedaAPIError(err error) error {
	if err == nil {
		return nil
	}
	if isMissingKedaAPI(err) {
		return fmt.Errorf("keda ScaledObject CRD not available in the cluster: %w", err)
	}
	return err
}

func createScaledObject(service types.Service, namespace string, cfg *types.Config) error {
	if err := validateKedaConfig(service); err != nil {
		return err
	}

	gatewayClientset, err := gatewayClientsetProvider()
	if err != nil {
		return err
	}

	scaledObject := getScaledObjectSpec(service, namespace, cfg)
	_, err = gatewayClientset.Resource(scaledObjectGVR).Namespace(namespace).Create(context.TODO(), scaledObject, metav1.CreateOptions{})
	return wrapMissingKedaAPIError(err)
}

func updateScaledObject(service types.Service, namespace string, cfg *types.Config) error {
	if err := validateKedaConfig(service); err != nil {
		return err
	}

	gatewayClientset, err := gatewayClientsetProvider()
	if err != nil {
		return err
	}

	scaledObject := getScaledObjectSpec(service, namespace, cfg)
	current, err := gatewayClientset.Resource(scaledObjectGVR).Namespace(namespace).Get(context.TODO(), scaledObject.GetName(), metav1.GetOptions{})
	if err != nil {
		return wrapMissingKedaAPIError(err)
	}
	scaledObject.SetResourceVersion(current.GetResourceVersion())

	_, err = gatewayClientset.Resource(scaledObjectGVR).Namespace(namespace).Update(context.TODO(), scaledObject, metav1.UpdateOptions{})
	return wrapMissingKedaAPIError(err)
}

func deleteScaledObject(name string, namespace string) error {
	gatewayClientset, err := gatewayClientsetProvider()
	if err != nil {
		return err
	}

	err = gatewayClientset.Resource(scaledObjectGVR).Namespace(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	return ignoreNotFound(err)
}

func existsScaledObject(serviceName string, namespace string) bool {
	gatewayClientset, err := gatewayClientsetProvider()
	if err != nil {
		return false
	}

	_, err = gatewayClientset.Resource(scaledObjectGVR).Namespace(namespace).Get(context.TODO(), getScaledObjectName(serviceName), metav1.GetOptions{})
	return err == nil
}

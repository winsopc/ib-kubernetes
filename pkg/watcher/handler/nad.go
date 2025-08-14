// Copyright 2025 NVIDIA CORPORATION & AFFILIATES
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"encoding/json"
	"fmt"
	"sync"

	v1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/Mellanox/ib-kubernetes/pkg/utils"
)

type NADEventHandler struct {
	addedNADs   *utils.SynchronizedMap // Maps network ID to NAD for added/updated NADs
	deletedNADs *utils.SynchronizedMap // Maps network ID to NAD for deleted NADs
	nadCache    sync.Map               // Cache of current NADs by namespace/name
}

// NewNADEventHandler creates a new NAD event handler
func NewNADEventHandler() ResourceEventHandler {
	return &NADEventHandler{
		addedNADs:   utils.NewSynchronizedMap(),
		deletedNADs: utils.NewSynchronizedMap(),
		nadCache:    sync.Map{},
	}
}

// GetResourceObject returns the NAD object type for the watcher
func (n *NADEventHandler) GetResourceObject() runtime.Object {
	return &v1.NetworkAttachmentDefinition{
		TypeMeta: metav1.TypeMeta{
			Kind:       "NetworkAttachmentDefinition",
			APIVersion: "k8s.cni.cncf.io/v1",
		},
	}
}

// OnAdd handles NAD creation events
func (n *NADEventHandler) OnAdd(obj interface{}, _ bool) {
	nad := obj.(*v1.NetworkAttachmentDefinition)
	log.Info().Msgf("NAD add event: namespace %s name %s", nad.Namespace, nad.Name)

	// Only handle InfiniBand SR-IOV networks
	if !n.isInfiniBandNetwork(nad) {
		log.Debug().Msgf("NAD %s/%s is not an InfiniBand network", nad.Namespace, nad.Name)
		return
	}

	networkID := fmt.Sprintf("%s/%s", nad.Namespace, nad.Name)

	// Cache the NAD for future reference
	n.nadCache.Store(networkID, nad)

	// Add to processing queue
	n.addedNADs.Set(networkID, nad)

	log.Info().Msgf("Successfully processed NAD add event: %s", networkID)
}

// OnUpdate handles NAD modification events
func (n *NADEventHandler) OnUpdate(oldObj, newObj interface{}) {
	oldNAD := oldObj.(*v1.NetworkAttachmentDefinition)
	newNAD := newObj.(*v1.NetworkAttachmentDefinition)

	log.Info().Msgf("NAD update event: namespace %s name %s", newNAD.Namespace, newNAD.Name)

	// Only handle InfiniBand SR-IOV networks
	if !n.isInfiniBandNetwork(newNAD) {
		log.Debug().Msgf("NAD %s/%s is not an InfiniBand network", newNAD.Namespace, newNAD.Name)
		return
	}

	networkID := fmt.Sprintf("%s/%s", newNAD.Namespace, newNAD.Name)

	// Check if configuration changed (e.g., PKey change)
	if n.hasConfigurationChanged(oldNAD, newNAD) {
		log.Info().Msgf("NAD configuration changed for %s", networkID)

		// Update cache
		n.nadCache.Store(networkID, newNAD)

		// Add to processing queue for reconfiguration
		n.addedNADs.Set(networkID, newNAD)

		log.Info().Msgf("Successfully processed NAD update event: %s", networkID)
	} else {
		log.Debug().Msgf("No significant changes in NAD %s", networkID)
	}
}

// OnDelete handles NAD deletion events
func (n *NADEventHandler) OnDelete(obj interface{}) {
	nad := obj.(*v1.NetworkAttachmentDefinition)
	log.Info().Msgf("NAD delete event: namespace %s name %s", nad.Namespace, nad.Name)

	networkID := fmt.Sprintf("%s/%s", nad.Namespace, nad.Name)

	// Remove from cache
	n.nadCache.Delete(networkID)

	// Only handle InfiniBand SR-IOV networks
	if !n.isInfiniBandNetwork(nad) {
		log.Debug().Msgf("NAD %s/%s is not an InfiniBand network", nad.Namespace, nad.Name)
		return
	}

	// Add to deletion queue
	n.deletedNADs.Set(networkID, nad)

	log.Info().Msgf("Successfully processed NAD delete event: %s", networkID)
}

// GetResults returns the results maps for processing by the daemon
func (n *NADEventHandler) GetResults() (*utils.SynchronizedMap, *utils.SynchronizedMap) {
	return n.addedNADs, n.deletedNADs
}

// GetNADFromCache retrieves a cached NAD by network ID
func (n *NADEventHandler) GetNADFromCache(networkID string) (*v1.NetworkAttachmentDefinition, bool) {
	if nad, ok := n.nadCache.Load(networkID); ok {
		return nad.(*v1.NetworkAttachmentDefinition), true
	}
	return nil, false
}

// isInfiniBandNetwork checks if the NAD is for InfiniBand SR-IOV
func (n *NADEventHandler) isInfiniBandNetwork(nad *v1.NetworkAttachmentDefinition) bool {
	// Parse the network configuration
	var networkConfig map[string]interface{}
	if err := json.Unmarshal([]byte(nad.Spec.Config), &networkConfig); err != nil {
		log.Error().Msgf("Failed to parse NAD config for %s/%s: %v", nad.Namespace, nad.Name, err)
		return false
	}

	// Check if this is an ib-sriov network
	if cniType, ok := networkConfig["type"]; ok {
		return cniType == utils.InfiniBandSriovCni
	}

	return false
}

// hasConfigurationChanged checks if the NAD configuration has changed
func (n *NADEventHandler) hasConfigurationChanged(oldNAD, newNAD *v1.NetworkAttachmentDefinition) bool {
	// Compare relevant fields that affect network configuration
	if oldNAD.Spec.Config != newNAD.Spec.Config {
		return true
	}

	// Check for resource version changes (indicates metadata updates)
	if oldNAD.ResourceVersion != newNAD.ResourceVersion {
		return true
	}

	return false
}

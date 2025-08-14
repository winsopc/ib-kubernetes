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

package daemon

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	v1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netAttUtils "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/utils"
	"github.com/rs/zerolog/log"
	kapi "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/Mellanox/ib-kubernetes/pkg/config"
	"github.com/Mellanox/ib-kubernetes/pkg/guid"
	k8sClient "github.com/Mellanox/ib-kubernetes/pkg/k8s-client"
	"github.com/Mellanox/ib-kubernetes/pkg/sm"
	"github.com/Mellanox/ib-kubernetes/pkg/sm/plugins"
	"github.com/Mellanox/ib-kubernetes/pkg/utils"
	"github.com/Mellanox/ib-kubernetes/pkg/watcher"
	resEvenHandler "github.com/Mellanox/ib-kubernetes/pkg/watcher/handler"
)

type Daemon interface {
	// Execute Daemon loop, returns when os.Interrupt signal is received
	Run()
}

type daemon struct {
	config            config.DaemonConfig
	podWatcher        watcher.Watcher
	nadWatcher        watcher.Watcher // NAD watcher for network definition changes
	kubeClient        k8sClient.Client
	guidPool          guid.Pool
	smClient          plugins.SubnetManagerClient
	guidPodNetworkMap map[string]string // allocated guid mapped to the pod and network

	// NAD tracking state
	podNetworkCache map[string][]string                        // pod UID -> list of network IDs
	networkPodCache map[string][]string                        // network ID -> list of pod UIDs
	nadCache        map[string]*v1.NetworkAttachmentDefinition // network ID -> NAD
}

// Temporary struct used to proceed pods' networks
type podNetworkInfo struct {
	pod       *kapi.Pod
	ibNetwork *v1.NetworkSelectionElement
	networks  []*v1.NetworkSelectionElement
	addr      net.HardwareAddr // GUID allocated for ibNetwork and saved as net.HardwareAddr
}

type networksMap struct {
	theMap map[types.UID][]*v1.NetworkSelectionElement
}

// Exponential backoff ~26 sec + 6 * <api call time>
// NOTE: k8s client has built in exponential backoff, which ib-kubernetes don't use.
// In case client's backoff was configured time may dramatically increase.
// NOTE: ufm client has default timeout on request operation for 30 seconds.
var backoffValues = wait.Backoff{Duration: 1 * time.Second, Factor: 1.6, Jitter: 0.1, Steps: 6}

// Return networks mapped to the pod. If mapping not exist it is created
func (n *networksMap) getPodNetworks(pod *kapi.Pod) ([]*v1.NetworkSelectionElement, error) {
	var err error
	networks, ok := n.theMap[pod.UID]
	if !ok {
		networks, err = netAttUtils.ParsePodNetworkAnnotation(pod)
		if err != nil {
			return nil, fmt.Errorf("failed to read pod networkName annotations pod namespace %s name %s, with error: %v",
				pod.Namespace, pod.Name, err)
		}

		n.theMap[pod.UID] = networks
	}
	return networks, nil
}

// NewDaemon initializes the need components including k8s client, subnet manager client plugins, and guid pool.
// It returns error in case of failure.
func NewDaemon() (Daemon, error) {
	daemonConfig := config.DaemonConfig{}
	if err := daemonConfig.ReadConfig(); err != nil {
		return nil, err
	}

	if err := daemonConfig.ValidateConfig(); err != nil {
		return nil, err
	}

	podEventHandler := resEvenHandler.NewPodEventHandler()
	nadEventHandler := resEvenHandler.NewNADEventHandler()
	client, err := k8sClient.NewK8sClient()
	if err != nil {
		return nil, err
	}

	pluginLoader := sm.NewPluginLoader()
	getSmClientFunc, err := pluginLoader.LoadPlugin(path.Join(
		daemonConfig.PluginPath, daemonConfig.Plugin+".so"), sm.InitializePluginFunc)
	if err != nil {
		return nil, err
	}

	smClient, err := getSmClientFunc()
	if err != nil {
		return nil, err
	}

	// Try to validate if subnet manager is reachable in backoff loop
	var validateErr error
	if err := wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		if err := smClient.Validate(); err != nil {
			log.Warn().Msgf("%v", err)
			validateErr = err
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, validateErr
	}

	guidPool, err := guid.NewPool(&daemonConfig.GUIDPool)
	if err != nil {
		return nil, err
	}

	// Reset guid pool with already allocated guids to avoid collisions
	err = syncGUIDPool(smClient, guidPool)
	if err != nil {
		return nil, err
	}

	podWatcher := watcher.NewWatcher(podEventHandler, client)
	nadWatcher := watcher.NewWatcher(nadEventHandler, client)
	return &daemon{
		config:            daemonConfig,
		podWatcher:        podWatcher,
		nadWatcher:        nadWatcher,
		kubeClient:        client,
		guidPool:          guidPool,
		smClient:          smClient,
		guidPodNetworkMap: make(map[string]string),
		podNetworkCache:   make(map[string][]string),
		networkPodCache:   make(map[string][]string),
		nadCache:          make(map[string]*v1.NetworkAttachmentDefinition),
	}, nil
}

func (d *daemon) Run() {
	// setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Init the guid pool
	if err := d.initPool(); err != nil {
		log.Error().Msgf("initPool(): Daemon could not init the guid pool: %v", err)
		os.Exit(1)
	}

	// Run periodic tasks
	// closing the channel will stop the goroutines executed in the wait.Until() calls below
	stopPeriodicsChan := make(chan struct{})
	go wait.Until(d.AddPeriodicUpdate, time.Duration(d.config.PeriodicUpdate)*time.Second, stopPeriodicsChan)
	go wait.Until(d.DeletePeriodicUpdate, time.Duration(d.config.PeriodicUpdate)*time.Second, stopPeriodicsChan)
	go wait.Until(d.ProcessNADChanges, time.Duration(d.config.PeriodicUpdate)*time.Second, stopPeriodicsChan)
	defer close(stopPeriodicsChan)

	// Run both watchers in background
	podWatcherStopFunc := d.podWatcher.RunBackground()
	nadWatcherStopFunc := d.nadWatcher.RunBackground()
	defer podWatcherStopFunc()
	defer nadWatcherStopFunc()

	// Run until interrupted by os signals
	sig := <-sigChan
	log.Info().Msgf("Received signal %s. Terminating...", sig)
}

// If network identified by networkID is IbSriov return network name and spec
func (d *daemon) getIbSriovNetwork(networkID string) (string, *utils.IbSriovCniSpec, error) {
	_, networkName, err := utils.ParseNetworkID(networkID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse network id %s with error: %v", networkID, err)
	}

	// Try to get net-attach-def from cache first, then fallback to API
	netAttInfo, err := d.getCachedNAD(networkID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get network attachment %s: %v", networkName, err)
	}
	log.Debug().Msgf("networkName attachment %v", netAttInfo)

	networkSpec := make(map[string]interface{})
	err = json.Unmarshal([]byte(netAttInfo.Spec.Config), &networkSpec)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse networkName attachment %s with error: %v", networkName, err)
	}
	log.Debug().Msgf("networkName attachment spec %+v", networkSpec)

	ibCniSpec, err := utils.GetIbSriovCniFromNetwork(networkSpec)
	if err != nil {
		return "", nil, fmt.Errorf(
			"failed to get InfiniBand SR-IOV CNI spec from network attachment %+v, with error %v",
			networkSpec, err)
	}

	log.Debug().Msgf("ib-sriov CNI spec %+v", ibCniSpec)
	return networkName, ibCniSpec, nil
}

// Return pod network info
func getPodNetworkInfo(netName string, pod *kapi.Pod, netMap networksMap) (*podNetworkInfo, error) {
	networks, err := netMap.getPodNetworks(pod)
	if err != nil {
		return nil, err
	}

	var network *v1.NetworkSelectionElement
	network, err = utils.GetPodNetwork(networks, netName)
	if err != nil {
		return nil, fmt.Errorf("failed to get pod network spec for network %s with error: %v", netName, err)
	}

	return &podNetworkInfo{
		pod:       pod,
		networks:  networks,
		ibNetwork: network,
	}, nil
}

// Verify if GUID already exist for given network ID and allocates new one if not
func (d *daemon) allocatePodNetworkGUID(allocatedGUID, podNetworkID string, podUID types.UID) error {
	if mappedID, exist := d.guidPodNetworkMap[allocatedGUID]; exist {
		if podNetworkID != mappedID {
			return fmt.Errorf("failed to allocate requested guid %s, already allocated for %s",
				allocatedGUID, mappedID)
		}
	} else if err := d.guidPool.AllocateGUID(allocatedGUID); err != nil {
		return fmt.Errorf("failed to allocate GUID for pod ID %s, wit error: %v", podUID, err)
	} else {
		d.guidPodNetworkMap[allocatedGUID] = podNetworkID
	}

	return nil
}

// Allocate network GUID, update Pod's networks annotation and add GUID to the podNetworkInfo instance
func (d *daemon) processNetworkGUID(networkID string, spec *utils.IbSriovCniSpec, pi *podNetworkInfo) error {
	var guidAddr guid.GUID
	allocatedGUID, err := utils.GetPodNetworkGUID(pi.ibNetwork)
	podNetworkID := utils.GeneratePodNetworkID(pi.pod, networkID)
	if err == nil {
		// User allocated guid manually or Pod's network was rescheduled
		guidAddr, err = guid.ParseGUID(allocatedGUID)
		if err != nil {
			return fmt.Errorf("failed to parse user allocated guid %s with error: %v", allocatedGUID, err)
		}

		err = d.allocatePodNetworkGUID(allocatedGUID, podNetworkID, pi.pod.UID)
		if err != nil {
			return err
		}
	} else {
		guidAddr, err = d.guidPool.GenerateGUID()
		if err != nil {
			switch err {
			// If the guid pool is exhausted, need to sync with SM in case there are unsynced changes
			case guid.ErrGUIDPoolExhausted:
				err = syncGUIDPool(d.smClient, d.guidPool)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("failed to generate GUID for pod ID %s, with error: %v", pi.pod.UID, err)
			}
		}

		allocatedGUID = guidAddr.String()
		err = d.allocatePodNetworkGUID(allocatedGUID, podNetworkID, pi.pod.UID)
		if err != nil {
			return err
		}

		err = utils.SetPodNetworkGUID(pi.ibNetwork, allocatedGUID, spec.Capabilities["infinibandGUID"])
		if err != nil {
			return fmt.Errorf("failed to set pod network guid with error: %v ", err)
		}

		// Update Pod's network annotation here, so if network will be rescheduled we wouldn't allocate it again
		netAnnotations, err := json.Marshal(pi.networks)
		if err != nil {
			return fmt.Errorf("failed to dump networks %+v of pod into json with error: %v", pi.networks, err)
		}

		pi.pod.Annotations[v1.NetworkAttachmentAnnot] = string(netAnnotations)
	}

	// used GUID as net.HardwareAddress to use it in sm plugin which receive []net.HardwareAddress as parameter
	pi.addr = guidAddr.HardWareAddress()
	return nil
}

func syncGUIDPool(smClient plugins.SubnetManagerClient, guidPool guid.Pool) error {
	usedGuids, err := smClient.ListGuidsInUse()
	if err != nil {
		return err
	}

	// Reset guid pool with already allocated guids to avoid collisions
	err = guidPool.Reset(usedGuids)
	if err != nil {
		return err
	}
	return nil
}

// Update and set Pod's network annotation.
// If failed to update annotation, pod's GUID added into the list to be removed from Pkey.
func (d *daemon) updatePodNetworkAnnotation(pi *podNetworkInfo, removedList *[]net.HardwareAddr) error {
	if pi.ibNetwork.CNIArgs == nil {
		pi.ibNetwork.CNIArgs = &map[string]interface{}{}
	}

	(*pi.ibNetwork.CNIArgs)[utils.InfiniBandAnnotation] = utils.ConfiguredInfiniBandPod

	netAnnotations, err := json.Marshal(pi.networks)
	if err != nil {
		return fmt.Errorf("failed to dump networks %+v of pod into json with error: %v", pi.networks, err)
	}

	pi.pod.Annotations[v1.NetworkAttachmentAnnot] = string(netAnnotations)

	// Try to set pod's annotations in backoff loop
	if err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		if err = d.kubeClient.SetAnnotationsOnPod(pi.pod, pi.pod.Annotations); err != nil {
			if kerrors.IsNotFound(err) {
				return false, err
			}
			log.Warn().Msgf("failed to update pod annotations with err: %v", err)
			return false, nil
		}

		return true, nil
	}); err != nil {
		log.Error().Msgf("failed to update pod annotations")

		if err = d.guidPool.ReleaseGUID(pi.addr.String()); err != nil {
			log.Warn().Msgf("failed to release guid \"%s\" from removed pod \"%s\" in namespace "+
				"\"%s\" with error: %v", pi.addr.String(), pi.pod.Name, pi.pod.Namespace, err)
		} else {
			delete(d.guidPodNetworkMap, pi.addr.String())
		}

		*removedList = append(*removedList, pi.addr)
	}

	return nil
}

//nolint:nilerr
func (d *daemon) AddPeriodicUpdate() {
	log.Info().Msgf("running periodic add update")
	addMap, _ := d.podWatcher.GetHandler().GetResults()
	addMap.Lock()
	defer addMap.Unlock()
	// Contains ALL pods' networks
	netMap := networksMap{theMap: make(map[types.UID][]*v1.NetworkSelectionElement)}
	for networkID, podsInterface := range addMap.Items {
		log.Info().Msgf("processing network networkID %s", networkID)
		pods, ok := podsInterface.([]*kapi.Pod)
		if !ok {
			log.Error().Msgf(
				"invalid value for add map networks expected pods array \"[]*kubernetes.Pod\", found %T",
				podsInterface)
			continue
		}

		if len(pods) == 0 {
			continue
		}

		log.Info().Msgf("processing network networkID %s", networkID)
		networkName, ibCniSpec, err := d.getIbSriovNetwork(networkID)
		if err != nil {
			addMap.UnSafeRemove(networkID)
			log.Error().Msgf("droping network: %v", err)
			continue
		}

		var guidList []net.HardwareAddr
		var passedPods []*podNetworkInfo
		for _, pod := range pods {
			log.Debug().Msgf("pod namespace %s name %s", pod.Namespace, pod.Name)
			var pi *podNetworkInfo
			pi, err = getPodNetworkInfo(networkName, pod, netMap)
			if err != nil {
				log.Error().Msgf("%v", err)
				continue
			}
			if err = d.processNetworkGUID(networkName, ibCniSpec, pi); err != nil {
				log.Error().Msgf("%v", err)
				continue
			}

			guidList = append(guidList, pi.addr)
			passedPods = append(passedPods, pi)
		}

		// Get configured PKEY for network and add the relevant POD GUIDs as members of the PKey via Subnet Manager
		if ibCniSpec.PKey != "" && len(guidList) != 0 {
			var pKey int
			pKey, err = utils.ParsePKey(ibCniSpec.PKey)
			if err != nil {
				log.Error().Msgf("failed to parse PKey %s with error: %v", ibCniSpec.PKey, err)
				continue
			}

			// Try to add pKeys via subnet manager in backoff loop
			if err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
				if err = d.smClient.AddGuidsToPKey(pKey, guidList); err != nil {
					log.Warn().Msgf("failed to config pKey with subnet manager %s with error : %v",
						d.smClient.Name(), err)
					return false, nil
				}
				return true, nil
			}); err != nil {
				log.Error().Msgf("failed to config pKey with subnet manager %s", d.smClient.Name())
				continue
			}
		}

		// Update annotations for PODs that finished the previous steps successfully
		var removedGUIDList []net.HardwareAddr
		for _, pi := range passedPods {
			err = d.updatePodNetworkAnnotation(pi, &removedGUIDList)
			if err != nil {
				log.Error().Msgf("%v", err)
			} else {
				// Track pod-network relationship for successful pods
				networks := make([]string, 0, len(pi.networks))
				for _, net := range pi.networks {
					if utils.IsPodNetworkConfiguredWithInfiniBand(net) {
						networks = append(networks, utils.GenerateNetworkID(net))
					}
				}
				d.trackPodNetworkRelationship(pi.pod, networks)
			}
		}

		if ibCniSpec.PKey != "" && len(removedGUIDList) != 0 {
			// Already check the parse above
			pKey, _ := utils.ParsePKey(ibCniSpec.PKey)

			// Try to remove pKeys via subnet manager in backoff loop
			if err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
				if err = d.smClient.RemoveGuidsFromPKey(pKey, removedGUIDList); err != nil {
					log.Warn().Msgf("failed to remove guids of removed pods from pKey %s"+
						" with subnet manager %s with error: %v", ibCniSpec.PKey,
						d.smClient.Name(), err)
					return false, nil
				}
				return true, nil
			}); err != nil {
				log.Warn().Msgf("failed to remove guids of removed pods from pKey %s"+
					" with subnet manager %s", ibCniSpec.PKey, d.smClient.Name())
				continue
			}
		}

		addMap.UnSafeRemove(networkID)
	}
	log.Info().Msg("add periodic update finished")
}

// get GUID from Pod's network
func getPodGUIDForNetwork(pod *kapi.Pod, networkName string) (net.HardwareAddr, error) {
	networks, netErr := netAttUtils.ParsePodNetworkAnnotation(pod)
	if netErr != nil {
		return nil, fmt.Errorf("failed to read pod networkName annotations pod namespace %s name %s, with error: %v",
			pod.Namespace, pod.Name, netErr)
	}

	network, netErr := utils.GetPodNetwork(networks, networkName)
	if netErr != nil {
		return nil, fmt.Errorf("failed to get pod networkName spec %s with error: %v", networkName, netErr)
	}

	if !utils.IsPodNetworkConfiguredWithInfiniBand(network) {
		return nil, fmt.Errorf("network %+v is not InfiniBand configured", network)
	}

	allocatedGUID, netErr := utils.GetPodNetworkGUID(network)
	if netErr != nil {
		return nil, netErr
	}

	guidAddr, guidErr := net.ParseMAC(allocatedGUID)
	if guidErr != nil {
		return nil, fmt.Errorf("failed to parse allocated Pod GUID, error: %v", guidErr)
	}

	return guidAddr, nil
}

//nolint:nilerr
func (d *daemon) DeletePeriodicUpdate() {
	log.Info().Msg("running delete periodic update")
	_, deleteMap := d.podWatcher.GetHandler().GetResults()
	deleteMap.Lock()
	defer deleteMap.Unlock()
	for networkID, podsInterface := range deleteMap.Items {
		log.Info().Msgf("processing network networkID %s", networkID)
		pods, ok := podsInterface.([]*kapi.Pod)
		if !ok {
			log.Error().Msgf("invalid value for add map networks expected pods array \"[]*kubernetes.Pod\", found %T",
				podsInterface)
			continue
		}

		if len(pods) == 0 {
			continue
		}

		networkName, ibCniSpec, err := d.getIbSriovNetwork(networkID)
		if err != nil {
			deleteMap.UnSafeRemove(networkID)
			log.Warn().Msgf("droping network: %v", err)
			continue
		}

		var guidList []net.HardwareAddr
		var guidAddr net.HardwareAddr
		for _, pod := range pods {
			log.Debug().Msgf("pod namespace %s name %s", pod.Namespace, pod.Name)
			guidAddr, err = getPodGUIDForNetwork(pod, networkName)
			if err != nil {
				log.Error().Msgf("%v", err)
				continue
			}

			guidList = append(guidList, guidAddr)
		}

		if ibCniSpec.PKey != "" && len(guidList) != 0 {
			pKey, pkeyErr := utils.ParsePKey(ibCniSpec.PKey)
			if pkeyErr != nil {
				log.Error().Msgf("failed to parse PKey %s with error: %v", ibCniSpec.PKey, pkeyErr)
				continue
			}

			// Try to remove pKeys via subnet manager on backoff loop
			if err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
				if err = d.smClient.RemoveGuidsFromPKey(pKey, guidList); err != nil {
					log.Warn().Msgf("failed to remove guids of removed pods from pKey %s"+
						" with subnet manager %s with error: %v", ibCniSpec.PKey,
						d.smClient.Name(), err)
					return false, nil
				}
				return true, nil
			}); err != nil {
				log.Warn().Msgf("failed to remove guids of removed pods from pKey %s"+
					" with subnet manager %s", ibCniSpec.PKey, d.smClient.Name())
				continue
			}
		}

		for i, guidAddr := range guidList {
			if err = d.guidPool.ReleaseGUID(guidAddr.String()); err != nil {
				log.Error().Msgf("%v", err)
				continue
			}

			delete(d.guidPodNetworkMap, guidAddr.String())

			// Clean up pod-network relationship tracking
			if i < len(pods) {
				podUID := string(pods[i].UID)
				d.handlePodNetworkCleanup(podUID, networkID)
			}
		}
		deleteMap.UnSafeRemove(networkID)
	}

	log.Info().Msg("delete periodic update finished")
}

// ProcessNADChanges processes NAD add/update/delete events
func (d *daemon) ProcessNADChanges() {
	log.Debug().Msg("Processing NAD changes...")

	nadHandler := d.nadWatcher.GetHandler().(*resEvenHandler.NADEventHandler)
	addedNADs, deletedNADs := nadHandler.GetResults()

	// Process added/updated NADs
	addedNADs.Lock()
	for networkID, nad := range addedNADs.Items {
		nadObj := nad.(*v1.NetworkAttachmentDefinition)

		log.Info().Msgf("Processing NAD change for network: %s", networkID)

		// Update local NAD cache
		d.nadCache[networkID] = nadObj

		// Find all pods using this network and trigger reconfiguration
		if podUIDs, exists := d.networkPodCache[networkID]; exists {
			for _, podUID := range podUIDs {
				log.Info().Msgf("NAD change affects pod: %s", podUID)
				d.handlePodNetworkReconfiguration(podUID, networkID, nadObj)
			}
		}

		// Remove processed item
		addedNADs.UnSafeRemove(networkID)
	}
	addedNADs.Unlock()

	// Process deleted NADs
	deletedNADs.Lock()
	for networkID := range deletedNADs.Items {
		log.Info().Msgf("Processing NAD deletion for network: %s", networkID)

		// Remove from local cache
		delete(d.nadCache, networkID)

		// Handle pods that were using this network
		if podUIDs, exists := d.networkPodCache[networkID]; exists {
			for _, podUID := range podUIDs {
				log.Warn().Msgf("NAD deleted, cleaning up pod: %s", podUID)
				d.handlePodNetworkCleanup(podUID, networkID)
			}
			// Clean up the network->pod mapping
			delete(d.networkPodCache, networkID)
		}

		// Remove processed item
		deletedNADs.UnSafeRemove(networkID)
	}
	deletedNADs.Unlock()

	log.Debug().Msg("NAD changes processing completed")
}

// handlePodNetworkReconfiguration handles pod reconfiguration when NAD changes
func (d *daemon) handlePodNetworkReconfiguration(podUID, networkID string, nad *v1.NetworkAttachmentDefinition) {
	log.Info().Msgf("Reconfiguring pod %s for network %s", podUID, networkID)

	// Parse the new network configuration
	var networkSpec map[string]interface{}
	if err := json.Unmarshal([]byte(nad.Spec.Config), &networkSpec); err != nil {
		log.Error().Msgf("Failed to parse NAD config for %s: %v", networkID, err)
		return
	}

	ibCniSpec, err := utils.GetIbSriovCniFromNetwork(networkSpec)
	if err != nil {
		log.Error().Msgf("Failed to parse IB CNI spec from NAD %s: %v", networkID, err)
		return
	}

	// Check if we have a GUID allocated for this pod-network combination
	guidKey := podUID + ":" + networkID
	if currentGUID, exists := d.guidPodNetworkMap[guidKey]; exists {
		// Update subnet manager with new PKey if changed
		if ibCniSpec.PKey != "" {
			pKey, err := utils.ParsePKey(ibCniSpec.PKey)
			if err != nil {
				log.Error().Msgf("Failed to parse new PKey %s for network %s: %v", ibCniSpec.PKey, networkID, err)
				return
			}

			// Convert GUID string to hardware address
			guidAddr, err := net.ParseMAC(currentGUID)
			if err != nil {
				log.Error().Msgf("Failed to parse GUID %s as MAC address: %v", currentGUID, err)
				return
			}

			// Add GUID to new PKey
			if err := d.smClient.AddGuidsToPKey(pKey, []net.HardwareAddr{guidAddr}); err != nil {
				log.Error().Msgf("Failed to add GUID %s to new PKey %d: %v", currentGUID, pKey, err)
				return
			}

			log.Info().Msgf("Successfully updated PKey for pod %s network %s to %s", podUID, networkID, ibCniSpec.PKey)
		}
	}
}

// handlePodNetworkCleanup handles cleanup when NAD is deleted
func (d *daemon) handlePodNetworkCleanup(podUID, networkID string) {
	log.Info().Msgf("Cleaning up pod %s for deleted network %s", podUID, networkID)

	// Remove GUID allocation
	guidKey := podUID + ":" + networkID
	if guid, exists := d.guidPodNetworkMap[guidKey]; exists {
		// Convert GUID string to hardware address for SM operations
		if _, err := net.ParseMAC(guid); err != nil {
			log.Error().Msgf("Failed to parse GUID %s as MAC address: %v", guid, err)
		} else {
			// Note: We can't remove from specific PKey since NAD is deleted
			// The subnet manager should handle orphaned GUIDs
			log.Info().Msgf("GUID %s for pod %s network %s will be cleaned up by subnet manager", guid, podUID, networkID)
		}

		// Return GUID to pool
		if err := d.guidPool.ReleaseGUID(guid); err != nil {
			log.Error().Msgf("Failed to release GUID %s to pool: %v", guid, err)
		}

		// Remove from tracking
		delete(d.guidPodNetworkMap, guidKey)
	}

	// Update pod->network cache
	if networks, exists := d.podNetworkCache[podUID]; exists {
		// Remove this network from the pod's network list
		for i, netID := range networks {
			if netID == networkID {
				d.podNetworkCache[podUID] = append(networks[:i], networks[i+1:]...)
				break
			}
		}
	}
}

// trackPodNetworkRelationship maintains bidirectional mapping between pods and networks
func (d *daemon) trackPodNetworkRelationship(pod *kapi.Pod, networks []string) {
	podUID := string(pod.UID)

	// Update pod->networks mapping
	d.podNetworkCache[podUID] = networks

	// Update network->pods mapping
	for _, networkID := range networks {
		if pods, exists := d.networkPodCache[networkID]; exists {
			// Add pod to existing list if not already present
			found := false
			for _, existingPodUID := range pods {
				if existingPodUID == podUID {
					found = true
					break
				}
			}
			if !found {
				d.networkPodCache[networkID] = append(pods, podUID)
			}
		} else {
			// Create new list with this pod
			d.networkPodCache[networkID] = []string{podUID}
		}
	}
}

// getCachedNAD retrieves NAD from cache, falling back to API if not cached
func (d *daemon) getCachedNAD(networkID string) (*v1.NetworkAttachmentDefinition, error) {
	// First check cache
	if nad, exists := d.nadCache[networkID]; exists {
		return nad, nil
	}

	// Fall back to API call (existing behavior)
	networkNamespace, networkName, err := utils.ParseNetworkID(networkID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse network id %s with error: %v", networkID, err)
	}

	var netAttInfo *v1.NetworkAttachmentDefinition
	if err = wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		netAttInfo, err = d.kubeClient.GetNetworkAttachmentDefinition(networkNamespace, networkName)
		if err != nil {
			log.Warn().Msgf("failed to get network attachment %s with error %v", networkName, err)
			return false, err
		}
		return true, nil
	}); err != nil {
		return nil, fmt.Errorf("failed to get network attachment %s", networkName)
	}

	// Cache the result
	d.nadCache[networkID] = netAttInfo

	return netAttInfo, nil
}

// initPool check the guids that are already allocated by the running pods
func (d *daemon) initPool() error {
	log.Info().Msg("Initializing GUID pool.")

	// Try to get pod list from k8s client in backoff loop
	var pods *kapi.PodList
	if err := wait.ExponentialBackoff(backoffValues, func() (bool, error) {
		var err error
		if pods, err = d.kubeClient.GetPods(kapi.NamespaceAll); err != nil {
			log.Warn().Msgf("failed to get pods from kubernetes: %v", err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		err = fmt.Errorf("failed to get pods from kubernetes")
		log.Error().Msgf("%v", err)
		return err
	}

	for index := range pods.Items {
		log.Debug().Msgf("checking pod for network annotations %v", pods.Items[index])
		pod := pods.Items[index]
		networks, err := netAttUtils.ParsePodNetworkAnnotation(&pod)
		if err != nil {
			continue
		}

		for _, network := range networks {
			if !utils.IsPodNetworkConfiguredWithInfiniBand(network) {
				continue
			}

			podGUID, err := utils.GetPodNetworkGUID(network)
			if err != nil {
				continue
			}
			podNetworkID := string(pod.UID) + network.Name
			if _, exist := d.guidPodNetworkMap[podGUID]; exist {
				if podNetworkID != d.guidPodNetworkMap[podGUID] {
					return fmt.Errorf("failed to allocate requested guid %s, already allocated for %s",
						podGUID, d.guidPodNetworkMap[podGUID])
				}
				continue
			}

			if err = d.guidPool.AllocateGUID(podGUID); err != nil {
				err = fmt.Errorf("failed to allocate guid for running pod: %v", err)
				log.Error().Msgf("%v", err)
				continue
			}

			d.guidPodNetworkMap[podGUID] = podNetworkID
		}
	}

	return nil
}

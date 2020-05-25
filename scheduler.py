#!/usr/bin/env python3

import time
from kubernetes import client, config
from baseclasses.placeholder import Placeholder
from baseclasses.pod import Pod
from baseclasses.node import Node
from baseclasses.scheduler import BaseScheduler
from baseclasses.cluster import DynamicClusterer
from configs import config as custom_config
import os
import json
import numpy as np
from logger import write_result
from offline_orchestrator import OfflineOrchestrator


class CustomScheduler(BaseScheduler):
    """CustomScheduler class that provide ultra-reliability and latency-awareness scheduling for Pods

    """

    def __init__(self, simulation, delay_matrix, metadata, nodes=None,
                 scheduler_name=custom_config.SCHEDULER_NAME, clusterer=None):
        """Constructor of CustomScheduler

        Args:
            simulation (bool): True if the underlying system is only simulated
            delay_matrix (numpy.ndarray): The matrix that holds the latency values between nodes
            metadata (dict[str, int]): Since numpy.ndarray cannot contain labels, we store the indexes of the nodes
            nodes (Optional[str, dict[str, baseclasses.node.Node], None]):
                None, if nodes are coming from Kubernetes
                str, JSON file path that contains the nodes
                dictionary, the already parsed nodes
            scheduler_name (str): The name of the scheduler that has to be referenced during a deplyoment
            clusterer (baseclasses.cluster.DynamicClusterer):
                DynamicClusterer object, if already constructed
                None anyway
        """
        self._delay_matrix = delay_matrix
        self._dm_metadata = metadata
        self._reverse_metadata = {value: key for key, value in self._dm_metadata.items()}
        self.simulation = simulation
        if not self.simulation:
            self.load_config()
            self.v1 = client.CoreV1Api()
            self.nodes = self.convert_kubernetes_nodes_to_my_nodes()
        else:
            if type(nodes) == str:
                self.nodes = self.convert_simulated_nodes_to_my_nodes(nodes)
            elif nodes is not None:
                self.nodes = nodes
        self.scheduler_name = scheduler_name
        self.placeholders, self.pods, self.reschedules = dict(), dict(), dict()
        self.temp_metadata, self.reverse_temp_metadata = None, None

        if len(self.nodes) != len(self._delay_matrix):
            raise Exception
        self.clusters = None
        if custom_config.CLUSTERING:
            print('Creating cluster layers')
            if clusterer is None:
                self.clusterer = DynamicClusterer(self.delay_matrix, self._dm_metadata)
            else:
                self.clusterer = clusterer
            if custom_config.INITIAL_CLUSTER_LAYERS is not None and len(custom_config.INITIAL_CLUSTER_LAYERS) > 0:
                self.clusterer.new_services(custom_config.INITIAL_CLUSTER_LAYERS)
                print('Cluster layers created')

    def convert_simulated_nodes_to_my_nodes(self, simulated_nodes_path):
        """Parse the file(s), that contain the serialized node objects

        Args:
            simulated_nodes_path (str): Path of the JSON file(s)

        Returns:
            (dict[str, baseclasses.node.Node]):
                Returns the dictionary where a key is the name of the node and the value is baseclasses.node.Node object
        """
        nodes = []
        if os.path.isdir(simulated_nodes_path):
            if simulated_nodes_path[-1] != '/':
                simulated_nodes_path += '/'
            node_files = [simulated_nodes_path + f for f in os.listdir(simulated_nodes_path) if
                          os.path.isfile(simulated_nodes_path + f)]
        else:
            node_files = [simulated_nodes_path]
        for node_file in node_files:
            with open(node_file, 'r') as f:
                json_data = f.read()
                node_json = json.loads(json_data)
            if isinstance(node_json, list):
                nodes += node_json
            else:
                nodes.append(node_json)
        my_nodes = {n['name']: Node(name=n['name'], k8s_node=None, memory=self.convert_to_int(n['allocatable_memory']),
                                    cloud=n['cloud']) for n in nodes}
        return my_nodes

    def update_delay_matrix(self, delay_matrix, metadata):
        """Update the metadata and the delay matrix according to the mesurement results

        Args:
            delay_matrix (numpy.ndarray): The matrix that holds the actual latency values between nodes
            metadata (dict[str, int]): Since numpy.ndarray cannot contain labels, we store the indexes of the nodes
        """
        if metadata != self._dm_metadata:
            if len(metadata) < len(self._dm_metadata):
                # Node failure happened
                self.node_failure_detection(self._dm_metadata, metadata)
            elif len(metadata) > len(self._dm_metadata):
                # New Node in the cluster
                self.new_node_detection(self._dm_metadata, metadata)
        self.temp_metadata = metadata
        self.reverse_temp_metadata = {value: key for key, value in self.temp_metadata.items()}
        self.delay_matrix = delay_matrix
        self._dm_metadata = metadata
        self._reverse_metadata = self.reverse_temp_metadata
        self.temp_metadata = None
        self.reverse_temp_metadata = None
        if custom_config.CLUSTERING:
            self.update_clustering()
        self.check_all_constraints()

    def node_failure_detection(self, old_metadata, new_metadata):
        """Detect which node is unreachable currently

        Args:
            old_metadata (dict[str, int]): Metadata dictionary with old node names and indexes
            new_metadata (dict[str, int]): Metadata dictionary with new node names and indexes
        """
        missing_node_name = next(x for x in old_metadata if x not in new_metadata)
        print('\n %s node failure!' % missing_node_name)
        missing_node = self.get_node_from_name(missing_node_name)
        missing_node.alive = False
        for pod_name, pod in self.pods.items():
            if pod.node == missing_node_name:
                placeholder = self.get_placeholder_by_pod(pod.name)
                if placeholder is not None:
                    # The Pod has already an assigned placeholder so probably a node failure occurred,
                    # we need to restart the pod
                    self.patch_pod(pod, placeholder.node)

    def new_node_detection(self, old_metadata, new_metadata):
        """Detect the new node in the cluster

        Args:
            old_metadata (dict[str, int]): Metadata dictionary with old node names and indexes
            new_metadata (dict[str, int]): Metadata dictionary with new node names and indexes

        Returns:

        """
        new_node_name = next(x for x in new_metadata if x not in old_metadata)
        new_node = self.get_node_from_name(new_node_name)
        if new_node is not None:
            print('%s node is online again!' % new_node_name)
            new_node.alive = True
        else:
            print('New node in the cluster!')

    @property
    def delay_matrix(self):
        """The delay matrix is a numpy.ndarray that contains all latency values between each node

        """
        return self._delay_matrix

    @delay_matrix.setter
    def delay_matrix(self, delay_matrix):
        if self._delay_matrix is not None:  # A delay matrix is already present, so check if any changes happened
            nothing_changed = True
            # Getting the original nodeX-nodeY-delay triplets in a list
            original_triplets = []
            for i in range(len(self._delay_matrix)):
                for j in range(len(self._delay_matrix)):
                    original_triplets.append({self._reverse_metadata[i], self._reverse_metadata[j],
                                              int(self._delay_matrix[i, j])})
            # Getting the new nodeX-nodeY-delay triplets in a list
            new_triplets = []
            for i in range(len(delay_matrix)):
                for j in range(len(delay_matrix)):
                    new_triplets.append({self.reverse_temp_metadata[i], self.reverse_temp_metadata[j],
                                         int(delay_matrix[i, j])})
            # Check if any difference is present between the original and the new one
            if any(x not in new_triplets for x in original_triplets) or \
                    any(x not in original_triplets for x in new_triplets):
                nothing_changed = False

            if not nothing_changed:
                # Change the available lists if any significant change happened
                if len(self._delay_matrix) == len(delay_matrix):
                    # Delay change happened
                    print('Delay changed between two node!')
                    for pod_name, pod in self.pods.items():
                        if pod.constraint != {} and pod.name not in self.reschedules:
                            # We save the nodes in the radius only if pod has constraint and
                            # currently not under migration
                            previous_element_name = list(pod.constraint.keys())[0]
                            required_delay = pod.constraint[previous_element_name]
                            pod.nodes_in_radius = [name
                                                   for name, x in self.get_nodes_in_radius(previous_element_name,
                                                                                           required_delay).items()]
                self._delay_matrix = delay_matrix
        self._delay_matrix = delay_matrix

    def update_clustering(self):
        """Update the clusters and cluster layers with aware of the new delay values

        """
        # FIXME: Dummy solution
        print('Updating cluster layers')
        previous_delays = list(self.clusterer.clustering_layers.keys())
        self.clusterer = DynamicClusterer(self.delay_matrix, self._dm_metadata)
        for d in previous_delays:
            self.clusterer.new_service(d)
        print('Cluster layers updated')

    @staticmethod
    def load_config():
        """Loading the Kubernetes config

        """
        try:
            config.load_kube_config()
        except FileNotFoundError as e:
            print("WARNING %s\n" % e)
            config.load_incluster_config()

    def k8s_nodes_available(self, only_name=True):
        """Get the nodes with status.type=Ready from Kubernetes

        Args:
            only_name (bool): Store only the name of the node

        Returns:
            (Optional[list[kubernetes.client.V1Node], list[str]]): List of the nodes/node names that are Ready
        """
        ready_nodes = []
        for n in self.v1.list_node().items:
            if not n.spec.unschedulable:
                for status in n.status.conditions:
                    if status.status == "True" and status.type == "Ready":
                        if only_name:
                            ready_nodes.append(n.metadata.name)
                        else:
                            ready_nodes.append(n)
        return ready_nodes

    def nodes_available(self):
        """Get the nodes that are alive

        Returns:
            (dict[str, baseclasses.node.Node]): Dictionary with node names and Node objects, that are alive
        """
        return {name: node for name, node in self.nodes.items() if node.alive}

    def get_all_k8s_pods(self, kube_system=False):
        """Get the Pods from Kubernetes API

        Args:
            kube_system (bool): False if we exclude Pods that are in kube-system namespace

        Returns:
            (list[kubernetes.client.V1Pod]): List with Pods from Kubernetes
        """
        if not kube_system:
            return [x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if
                    x.metadata.namespace != 'kube-system']
        else:
            return self.v1.list_pod_for_all_namespaces(watch=False).items

    def get_k8s_pod_by_name(self, name, node=None):
        """Query the Kubernetes API fro a Pod with the given name

        Args:
            name (str): Pod's name
            node (str): Pod's host name (Optional)

        Returns:
            (kubernetes.client.v1Pod): The Pod object with the given name
        """
        pods = []
        while len(pods) == 0:
            if node is None:
                pods = [x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if
                        name in x.metadata.name and x.metadata.deletion_timestamp is None]
            else:
                pods = [x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if
                        name in x.metadata.name and x.metadata.deletion_timestamp is None and x.spec.node_name == node]
        pod = max(pods, key=lambda x: x.metadata.creation_timestamp)
        return pod

    def get_pods_on_node(self, node_name):
        """Get all the Pod objects that are deployed on a node with the given name

        Args:
            node_name (str): The name of the node

        Returns:
            (list[baseclasses.pod.Pod]): The list of the Pods objects deployed to that appropriate node
        """
        node = self.get_node_from_name(node_name)
        return [self.get_pod_by_name(x) for x in node.pods]

    @staticmethod
    def convert_to_int(resource_string):
        """Convert resource strings to integer (bytes)

        Args:
            resource_string (str): Resource string

        Returns:
            (int): The number of bytes converted from the resource string

        Examples:
            >>> print(CustomScheduler.convert_to_int('2Gi'))
            2147483648
        """
        if 'Ki' in resource_string:
            return int(resource_string.split('K')[0])*1024
        elif 'Mi' in resource_string:
            return int(resource_string.split('M')[0])*(1024**2)
        elif 'Gi' in resource_string:
            return int(resource_string.split('G')[0])*(1024**3)

    def get_cloud_nodes(self):
        """Get the baseclass.node.Node objects that represent the cloud resources

        Returns:
            (dict[str, baseclasses.node.Node]): Nodes that are in the cloud
        """
        return {name: node for name, node in self.nodes.items() if node.cloud}

    def get_k8s_cloud_nodes(self):
        """Get the kubernetes.client.V1Node objects whose role is cloud

        Returns:
            (list[kubernetes.client.V1Node]): Nodes that are in the cloud
        """
        return [x for x in self.v1.list_node().items if 'kubernetes.io/role' in x.metadata.labels.keys() and
                                                        x.metadata.labels['kubernetes.io/role'] == 'cloud']

    def get_nodes_in_radius(self, previous_element_name, required_delay):
        """Get the nodes that are in the radius of the previous element with the given delay value
        If clustering enabled, we get the cluster/nodes from the baseclasses.cluster.DynamicClusterer object.

        Args:
            previous_element_name (str): The name of the previous element. Can be Pod's name or node's name
            required_delay (int): The delay value that gives the maximum distance from the previous element

        Returns:
            (dict[str, baseclasses.node.Node]): Dictionary that contains the nodes in the radius of the previous element
        """
        if not custom_config.CLUSTERING:
            return self.get_nodes_in_radius_without_clustering(previous_element_name, required_delay)
        else:
            previous_node = self.get_node_from_podname_or_nodename(previous_element_name)
            nodes_in_rad_from_clusterer = self.clusterer.get_nodes_in_radius(previous_node.name, required_delay)
            nodes_in_radius = dict()
            for n in nodes_in_rad_from_clusterer:
                node = self.get_node_from_name(n)
                if node.alive:
                    nodes_in_radius[node.name] = node
            return nodes_in_radius

    def get_nodes_in_radius_without_clustering(self, previous_element_name, required_delay):
        """Get the nodes that are in the radius of the previous element with the given delay value

        Args:
            previous_element_name (str): The name of the previous element. Can be Pod's name or node's name
            required_delay (int): The delay value that gives the maximum distance from the previous element

        Returns:
            (dict[str, baseclasses.node.Node]): Dictionary that contains the nodes in the radius of the previous element
        """
        previous_node = self.get_node_from_podname_or_nodename(previous_element_name)
        nodes_in_radius = dict()
        nodes_in_radius[previous_node.name] = previous_node
        for index in np.where(self.delay_matrix[self._dm_metadata[previous_node.name]] <= required_delay)[0]:
            node = self.get_node_from_name(self._reverse_metadata[index])
            if node.alive:
                nodes_in_radius[node.name] = node
        return nodes_in_radius

    def reused_placeholder_unused_node(self, placeholder, nodes_enough_resource):
        """Examine if any node available in the given dictionary, that is not covered with the given placeholder

        Args:
            placeholder (baseclasses.placeholder.Placeholder): Placeholder object we examine against the nodes
            nodes_enough_resource (dict[str, baseclasses.node.Node]): The nodes we examine against the placeholder

        Returns:
            (bool, dict[str, baseclasses.node.Node]):
                (True, dictionary) if there is a node which is not covered by the placeholder
                (False, None) anyway
        """
        covered_nodes = dict()
        for pod in placeholder.pods:
            node = self.get_node_from_pod(pod)
            covered_nodes[node.name] = node
        if any(x not in covered_nodes and x != placeholder.node for x in nodes_enough_resource):
            covered_nodes[placeholder.node] = self.get_node_from_name(placeholder.node)
            return True, covered_nodes
        return False, None

    def get_memory_matrix(self, placeholder, nodes_enough_resource=None):
        """Calculate the memory matrix for a given placeholder and some nodes

        Args:
            placeholder (baseclasses.placeholder.Placeholder): The placeholder we examine against the nodes
            nodes_enough_resource (dict[str, baseclasses.node.Node]): The dictionary with the nodes we examine

        Returns:
            (dict[str, int]): Returning a dictionary which contains keys with the nodes' name and the values are
                the sum of the Pods' memory request that covered by the given placeholder on the node
        """
        placeholder_memory_matrix = {}
        if nodes_enough_resource is None:
            nodes = self.nodes_available()
        else:
            nodes = nodes_enough_resource
        for n, node in nodes.items():
            if n != placeholder.node:
                placeholder_memory_matrix[n] = 0
                for pod in self.get_pods_on_node(n):
                    if pod.name in placeholder.pods:
                        placeholder_memory_matrix[n] += pod.memory_request
        return placeholder_memory_matrix

    def reused_placeholder_used_pod_node(self, placeholder, pod_mem_req, nodes_enough_resource):
        """Examine if any node available in the given dictionary, that is covered with the given placeholder,
        but no need to increase the placeholder size, if the Pod with the given memory request would be deployed
        on one of the nodes

        Args:
            placeholder (baseclasses.placeholder.Placeholder): Placeholder object we examine against the nodes
            pod_mem_req (int): The Pod's memory request
            nodes_enough_resource (dict[str, baseclasses.node.Node]): Nodes we examine against the placeholder

        Returns:
            (bool, dict[str, baseclasses.node.Node]):
                (True, dictionary) if there is at least one node that can hold the Pod
                without increasing the placeholder's size
                (Flase, None) anyway
        """
        placeholder_memory_matrix = self.get_memory_matrix(placeholder, nodes_enough_resource)
        if any(placeholder_memory_matrix[x] + pod_mem_req <= placeholder.required_memory
               for x in placeholder_memory_matrix.keys()):
            return True, next({name: node} for name, node in nodes_enough_resource.items() if
                              name in placeholder_memory_matrix.keys()
                              and placeholder_memory_matrix[name] + pod_mem_req <= placeholder.required_memory)
        return False, None

    def create_new_placeholder(self, nodes_enough_resource, pod):
        """Creation of a new baseclasses.placeholder.Placeholder object

        Args:
            nodes_enough_resource (dict[str, baseclasses.node.Node]): Contains the nodes that can host the placeholder
            pod (baseclasses.pod.Pod): The Pod that will be covered by the new placeholder

        Returns:
            (baseclasses.placeholder.Placeholder): The created palceholder object
        """
        chosen_node = self.choose_min_utilized_by_mem(nodes_enough_resource)
        placeholder = Placeholder(node=chosen_node.name)
        self.add_pod_to_placeholder(pod, placeholder, pod.memory_request)
        self.placeholders[placeholder.id] = placeholder
        chosen_node.add_placeholder(placeholder)
        return placeholder

    @staticmethod
    def add_pod_to_placeholder(pod, placeholder, extra_memory=0):
        """Assign pod to placeholder

        Args:
            pod (baseclasses.pod.Pod): Pod object that will be assigned to the placeholder
            placeholder (baseclasses.placeholder.Placeholder): Placeholder object that covers the Pod
            extra_memory (int): The amount of extra memory allocation if necessary

        """
        placeholder.pods.add(pod.name)
        placeholder.required_memory += extra_memory

    def del_pod_from_placeholder(self, pod, placeholder):
        """Remove Pod from the given placeholder's covered pods

        Args:
            pod (baseclasses.pod.Pod): Pod object that will be removed from placeholder
            placeholder (baseclasses.placeholder.Placeholder): Placeholder object that covers the Pod

        """
        if isinstance(pod, str):
            pod_name = pod
        else:
            pod_name = pod.name
        placeholder.pods.remove(pod_name)
        mem_matrix = self.get_memory_matrix(placeholder=placeholder)
        node = self.get_node_from_name(placeholder.node)
        if len(placeholder.pods) == 0:
            node.delete_placeholder(placeholder)
            del self.placeholders[placeholder.id]
            return
        memories = []
        for k, v in mem_matrix.items():
            memories.append(v)
        if placeholder.required_memory != max(memories):
            memory_difference = max(memories) - placeholder.required_memory
            node.available_memory -= memory_difference
            placeholder.required_memory = max(memories)

    def try_to_assign_placeholder(self, pod_memory_request, nodes_less_resource, nodes_enough_resource):
        """Try to assign a previously created placeholder for the Pod defined by the given memory request and radius

        Args:
            pod_memory_request (int): The memory request of the Pod
            nodes_less_resource (dict[str, baseclasses.node.Node]): Node objects with insufficient amount of memory
            nodes_enough_resource (dict[str, baseclasses.node.Node]): Node objects with sufficient amount of memory

        Returns:
            (dict[str, baseclasses.node.Node], baseclasses.placeholder.Placeholder):
                (dict, Placeholder) if we can deploy the Pod on a node without increasing a placeholder's size
                ({}, None) anyway
        """
        placeholders_in_rad = [node.placeholder for name, node in nodes_less_resource.items()
                               if node.placeholder is not None] + \
                              [node.placeholder for name, node in nodes_enough_resource.items()
                               if node.placeholder is not None]

        for placeholder in placeholders_in_rad:
            # Looking for a node, which is in the radius and the actual placeholder has not any covered pod on it
            is_any_usable, excluded_dict = self.reused_placeholder_unused_node(placeholder, nodes_enough_resource)
            if is_any_usable:
                return {name: nodes_enough_resource[name] for name in nodes_enough_resource
                        if name not in excluded_dict}, placeholder

        for placeholder in placeholders_in_rad:
            # Looking for a node, which is in the radius and the actual placeholder has covered pod on it,
            # BUT
            # no need to increase the size of it
            is_any_usable, chosen_node_dict = self.reused_placeholder_used_pod_node(placeholder, pod_memory_request,
                                                                                    nodes_enough_resource)
            if is_any_usable:
                return chosen_node_dict, placeholder
        return {}, None
        #  or assign the pod somewhere in the radius ?

    def cover_pod_with_new_placeholder(self, nodes_enough_resource, pod):
        """Covering a Pod with a fresh placeholder
            It is possible that the Pod cannot be covered in the current state
        Args:
            nodes_enough_resource (dict[str, baseclasses.node.Node]): Nodes that has enough resource
            pod (baseclasses.pod.Pod): Pod object we want to cover

        Returns:

        """
        if len(nodes_enough_resource) >= 1:
            try:
                # Create new placeholder
                node_with_no_placeholder = next({x: nodes_enough_resource[x]} for x in nodes_enough_resource
                                                if not nodes_enough_resource[x].placeholder)
                placeholder = self.create_new_placeholder(node_with_no_placeholder, pod)
            except StopIteration:
                # Another option, when we have to increase the placeholder's size
                chosen_node = self.choose_min_utilized_by_mem(nodes_enough_resource)
                self.add_pod_to_placeholder(pod, chosen_node.placeholder, pod.memory_request)
                # TODO: we may not reduce node's memory by the size of the pod
                chosen_node.available_memory -= pod.memory_request
            return
        else:
            print("WARNING Cannot create placeholder for this pod!")
            write_result("WARNING Cannot create placeholder for this pod!", '')
            self.nodes[pod.node].delete_mypod(pod)
            del self.pods[pod.name]
            return

    def pod_has_placeholder(self, pod_name):
        """Get the placeholder of a Pod, if it present

        Args:
            pod_name (str): Name of the Pod

        Returns:
            (bool, baseclasses.placeholder.Placeholder):
                (True, Placeholder) if the Pod is covered with a placeholder
                (False, None) anyway
        """
        try:
            return True, next(self.placeholders[ph] for ph in self.placeholders if
                              pod_name in self.placeholders[ph].pods)
        except StopIteration:
            return False, None

    def get_k8s_pod_memory_request(self, pod):
        """Get the Pod's memory request through the Kubernetes API

        Args:
            (kubernetes.client.V1Pod): The Pod from Kubernetes

        Returns:
            (int): the summed container memory requests
        """
        return sum([self.convert_to_int(x.resources.requests['memory']) for x in pod.spec.containers if
                    x.resources.requests is not None])

    def narrow_nodes_by_capacity(self, pod_mem_req, nodes_dict):
        """Get the nodes that have enough free memory for the Pod

        Args:
            pod_mem_req (int): The amount of memory requested by the Pod
            nodes_dict (dict[str, baseclasses.node.Node]): Nodes that are examined

        Returns:
            (dict[str, baseclasses.node.Node]): Returns the nodes that have more (or equal) free memory
                than the requested
        """
        return_dict = dict()
        for name, node in nodes_dict.items():
            if node.available_memory >= pod_mem_req:
                return_dict[name] = node
        return return_dict

    def get_reschedulable(self, node, new_memory_request):
        """Get a Pod on the given node, that can be migrated to another location and frees enough memory for the new Pod

        Args:
            node (baseclasses.node.Node): The node we examine
            new_memory_request (int): The amount of memory requested by the new Pod

        Returns:
            (bool, baseclasses.pod.Pod, baseclasses.node.Node):
                (True, Pod, Node): The Pod we can migrate and its new host
                (False, None, None) means the node does not have any Pod that should be migrated for the new Pod
        """
        pods_on_node = self.get_pods_on_node(node.name)
        for old_pod in pods_on_node:
            if node.available_memory+old_pod.memory_request >= new_memory_request:
                old_start_point, old_required_delay = next((v, k) for v, k in old_pod.constraint.items())
                old_nodes_in_radius = self.narrow_nodes_by_capacity(old_pod.memory_request,
                                                                    self.get_nodes_in_radius(old_start_point,
                                                                                             old_required_delay))
                old_placeholder = self.get_placeholder_by_pod(old_pod)
                try:
                    del old_nodes_in_radius[old_placeholder.node]
                except KeyError:
                    pass
                if len(old_nodes_in_radius) > 0:
                    old_ph_mem_matrix = self.get_memory_matrix(old_placeholder, old_nodes_in_radius)
                    while len(old_nodes_in_radius) > 0:
                        chosen_node = self.choose_min_utilized_by_mem(old_nodes_in_radius)
                        if old_ph_mem_matrix[chosen_node.name] + old_pod.memory_request <= old_placeholder.required_memory:
                            return True, old_pod, chosen_node
                        else:
                            del old_nodes_in_radius[chosen_node.name]
        return False, None, None

    def migrate_pod(self, new_memory_request, new_nodes_in_radius):
        """Migration of a Pod

        Args:
            new_memory_request (int): The amount of memory requested by the new Pod
            new_nodes_in_radius (dict[str, baseclasses.node.Node]): The nodes that are in the radius of the new Pod

        Returns:
            (str): Returns the Pod name if it should be migrated for the new Pod, anyway it returns None
        """
        for name, node in new_nodes_in_radius.items():
            any_reschedulable, old_pod, reschedule_node = self.get_reschedulable(node, new_memory_request)
            if any_reschedulable:
                self.patch_pod(old_pod, reschedule_node.name)
                old_pod_name = old_pod.name
                if not self.simulation:
                    # We have to wait, while the pod get successfully rescheduled
                    # FIXME: We are waiting till only 1 instance remain. There can be more on purpose!
                    time.sleep(3)
                    print("INFO Waiting for rescheduling.")
                    while len([x.metadata.name for x in self.get_all_k8s_pods() if old_pod_name in x.metadata.name]) > 1:
                        time.sleep(1)
                        print("INFO Waiting for rescheduling.")
                else:
                    self.do_reschedule(old_pod)
                return old_pod_name
        return None

    def update_pod(self, pod, node):
        """Update the given Pod's hosting node

        Args:
            pod (baseclasses.pod.Pod): The Pod that will be updated
            node (baseclasses.node.Node): The node, which will be the new host for the Pod

        """
        if not self.simulation:
            pod.k8s_pod = self.get_k8s_pod_by_name(pod.name, node.name)
        pod.node = node.name
        node.add_mypod(pod)

    def do_reschedule(self, pod):
        """Reschedule the Pod to another node

        Args:
            pod (baseclasses.pod.Pod): Pod object that will be rescheduled

        """
        try:
            node_name = self.reschedules[pod.name]
            node = self.get_node_from_name(node_name)
            if not self.simulation:
                self.bind(pod.k8s_pod, node_name)
            pod_placeholder = self.get_placeholder_by_pod(pod.name)
            if pod_placeholder is not None and pod_placeholder.node == pod.node:
                # Node failure happened -> need to define new placeholder if possible
                self.del_pod_from_placeholder(pod.name, pod_placeholder)
                self.update_pod(pod, node)
                previous_element_name, required_delay = next((v, k) for v, k in pod.constraint.items())
                all_nodes_in_radius = self.get_nodes_in_radius(previous_element_name, required_delay)
                del all_nodes_in_radius[node.name]
                nodes_enough_resource_in_rad = self.narrow_nodes_by_capacity(pod.memory_request, all_nodes_in_radius)
                nodes_with_less_resource = {k: v for k, v in all_nodes_in_radius.items()
                                            if k not in nodes_enough_resource_in_rad}

                possible_nodes, new_placeholder = self.try_to_assign_placeholder(
                    pod_memory_request=pod.memory_request,
                    nodes_less_resource=nodes_with_less_resource,
                    nodes_enough_resource=nodes_enough_resource_in_rad)
                if new_placeholder is not None:
                    self.add_pod_to_placeholder(pod, new_placeholder)
                else:
                    self.cover_pod_with_new_placeholder(nodes_enough_resource_in_rad, pod)
            else:
                self.update_pod(pod, node)
        except StopIteration:
            # TODO: handle Pods in cloud reschedule
            pass

    def is_pod_constraint(self, previous_element_name):
        """Decide if the previous element is a Pod or a node

        Args:
            previous_element_name (str): Name of the previous element

        Returns:
            (bool): True if the pervious element is Pod, False anyway
        """
        return previous_element_name in self.pods

    def schedule(self, k8s_pod, namespace=custom_config.NAMESPACE):
        """The core scheduling method that parse the Kubernetes Pod and schedule it in the system

        Args:
            k8s_pod (kubernetes.client.V1Pod): The Pod from Kubernetes that will be scheduled
            namespace (Optional[str]): The namespace that will be used for deploying the Pod

        """
        try:
            pod_name = self.parse_podname(k8s_pod)
            if pod_name in self.reschedules.keys():
                pod = self.get_pod_by_name(pod_name)
                pod.k8s_pod = k8s_pod
                self.do_reschedule(pod)
                return
            # Is there any placeholder assigned to this pod name?
            any_assigned_placeholder, placeholder = self.pod_has_placeholder(pod_name)
            if any_assigned_placeholder:
                # The Pod has already an assigned placeholder so probably a node failure occurred,
                # we need to restart the pod
                pod = self.get_pod_by_name(pod_name)
                self.patch_pod(pod, placeholder.node)
                pod.k8s_pod = k8s_pod
                self.do_reschedule(pod)
                return
            else:
                # New Pod request
                # Get the previous element name from where the delay constraint defined
                previous_element_name = next(x for x in k8s_pod.metadata.labels.keys() if 'delay_' in x).split('_')[1]
                previous_element_is_pod = self.is_pod_constraint(previous_element_name)
                # Get the delay constraint value from the labels
                required_delay = int(k8s_pod.metadata.labels['delay_'+previous_element_name])
                # Getting all the nodes inside the delay radius
                all_nodes_in_radius = self.get_nodes_in_radius(previous_element_name, required_delay)
                pod_mem_req = self.get_k8s_pod_memory_request(k8s_pod)
                # print('{} node(s) in the radius'.format(str(len(all_nodes_in_radius))))
                nodes_enough_resource_in_rad = self.narrow_nodes_by_capacity(pod_mem_req, all_nodes_in_radius)
                # print('{} node(s) have enough resources'.format(str(len(nodes_enough_resource_in_rad))))
                if len(nodes_enough_resource_in_rad) == 0:
                    # There is no node with available resource
                    # Try to reschedule some previously deployed Pod
                    print('No node with available resource')
                    print('Try to reschedule some previously deployed Pod')
                    self.migrate_pod(pod_mem_req, all_nodes_in_radius)
                    # Recalculate the nodes with the computational resources
                    nodes_enough_resource_in_rad = self.narrow_nodes_by_capacity(pod_mem_req, all_nodes_in_radius)
                    if len(nodes_enough_resource_in_rad) == 0:
                        write_result('ERROR: Cannot place the requested pod', pod_name)
                        print('ERROR: Cannot place the requested pod', pod_name)
                        return

                nodes_with_less_resource = {x: node for x, node in all_nodes_in_radius.items()
                                            if x not in nodes_enough_resource_in_rad}
                possible_nodes = []
                # Choose a node for the pod
                placeholder = None
                if len(all_nodes_in_radius) > 1:
                    possible_nodes, placeholder = self.try_to_assign_placeholder(
                        pod_memory_request=pod_mem_req, nodes_less_resource=nodes_with_less_resource,
                        nodes_enough_resource=nodes_enough_resource_in_rad)
                if len(possible_nodes) > 0:
                    chosen_node = self.choose_min_utilized_by_mem(possible_nodes)
                else:
                    chosen_node = self.choose_min_utilized_by_mem(nodes_enough_resource_in_rad)
                constraint = {previous_element_name: required_delay}
                pod = self.place_pod(k8s_pod, chosen_node, (constraint, all_nodes_in_radius, previous_element_is_pod))
                if placeholder is not None:
                    self.add_pod_to_placeholder(pod, placeholder)
                if len(all_nodes_in_radius) > 1 and len(possible_nodes) == 0:
                    del nodes_enough_resource_in_rad[chosen_node.name]
                    self.cover_pod_with_new_placeholder(nodes_enough_resource_in_rad, pod)
                elif len(all_nodes_in_radius) == 1:
                    print("WARNING No placeholder will be assigned to this Pod!")
        except StopIteration:
            # No delay constraint for the pod
            nodes = self.get_cloud_nodes()
            node = self.choose_min_utilized_by_mem(nodes)
            self.place_pod(k8s_pod, node)

    def bind(self, k8s_pod, node_name, namespace=custom_config.NAMESPACE):
        """Bind the Pod to a node in Kubernetes using its API

        Args:
            k8s_pod (kubernetes.client.V1Pod): The Pod that will be binded
            node_name (str): The name of the node
            namespace (Optional[str]): The used namespace

        Returns:

        """
        # Must use V1Pod instance
        target = client.V1ObjectReference(api_version='v1', kind='Node', name=node_name)
        meta = client.V1ObjectMeta()
        meta.name = k8s_pod.metadata.name
        body = client.V1Binding(target=target, metadata=meta)
        try:
            api_response = self.v1.create_namespaced_pod_binding(name=k8s_pod.metadata.name, namespace=namespace,
                                                                 body=body)
            print(api_response)
            return api_response
        except Exception as e:
            # print("Warning when calling CoreV1Api->create_namespaced_pod_binding: %s\n" % e)
            pass

    def place_pod(self, k8s_pod, node, constraint=None):
        """Placing the Pod in the system

        Args:
            k8s_pod (kubernetes.client.V1Pod): Pod object that will be placed
            node (baseclasses.node.Node): The node which will host the Pod
            constraint (tuple[dict, dict, bool]): Pod's constraint, nodes in radius, and True if previous element is Pod

        Returns:
            (baseclasses.pod.Pod): The created and deployed baseclasses.pod.Pod object
        """
        pod_name = self.parse_podname(k8s_pod)
        mem_req = self.get_k8s_pod_memory_request(k8s_pod)
        if not self.simulation:
            self.bind(k8s_pod, node.name, custom_config.NAMESPACE)
        if constraint is None:
            pod = Pod(name=pod_name, node=node.name, k8s_pod=k8s_pod,
                      memory_request=mem_req, nodes_in_radius=[node.name],
                      cloudable=True)
        else:
            all_nodes_in_radius = constraint[1]
            previous_element_is_pod = constraint[2]
            pod = Pod(name=pod_name, node=node.name, k8s_pod=k8s_pod,
                      constraint=constraint[0], memory_request=mem_req,
                      nodes_in_radius=list(all_nodes_in_radius.keys()), pod_constraint=previous_element_is_pod)
        node.add_mypod(pod)
        self.pods[pod.name] = pod
        return pod

    @staticmethod
    def parse_podname(k8s_pod, splitting_char=custom_config.SPLITTING_CHAR, index=0):
        """Parsing the Pod's name

        Args:
            k8s_pod (kubernetes.client.V1Pod): The Pod object we examine
            splitting_char (str): The character used for splitting the Pod's name
            index (int): The index of the substring we use for name

        Returns:
            (str): Returns the name of the Pod
        """
        try:
            return k8s_pod.metadata.owner_references[0].name
        except:
            # FIXME: custom_config.SPLITTING_CHAR is assumed as splitting character
            return k8s_pod.metadata.name.split(splitting_char)[index]

    def patch_pod(self, pod, new_node_name, namespace=custom_config.NAMESPACE):
        """Mark a Pod as a reschedulable one and delete it from the system

        Args:
            pod (baseclasses.pod.Pod): The Pod we are patching
            new_node_name (str): The name of the new node
            namespace (str): The used namespace

        """
        self.reschedules[pod.name] = new_node_name
        old_node = self.get_node_from_name(pod.node)
        pod.node = new_node_name
        if not self.simulation:
            self.v1.delete_namespaced_pod(name=pod.k8s_pod.metadata.name, namespace=namespace)
        old_node.delete_mypod(pod)

    def delete(self, k8s_pod, namespace=custom_config.NAMESPACE):
        """Deleting the Pod from the system

        Args:
            k8s_pod (kubernetes.client.V1Pod): Pod, we want to delete
            namespace (str): The used namespace

        """
        pod_name = self.parse_podname(k8s_pod)
        if pod_name not in self.reschedules.keys():
            pod = self.get_pod_by_name(pod_name)
            placeholder = self.get_placeholder_by_pod(pod)
            if placeholder is not None:
                self.del_pod_from_placeholder(placeholder=placeholder, pod=pod)
            self.delete_pod(pod)
        else:
            del self.reschedules[pod_name]
            if len(list(self.reschedules.keys())) == 0:
                # Need to check other pods (with pod constraints) are still ok
                self.check_all_constraints()

    def delete_pod(self, pod):
        """Delete the Pod

        Args:
            pod (baseclasses.pod.Pod): The Pod we want to delete

        """
        node = self.get_node_from_podname_or_nodename(pod.name)
        node.delete_mypod(pod)
        del self.pods[pod.name]

    def convert_kubernetes_nodes_to_my_nodes(self):
        """Converting the nodes from Kubernetes API to baseclasses.node.Node objects

        Returns:
            (dict[str, baseclasses.node.Node objects]): Returns the dictionary with node_name: node object
        """
        nodes = self.k8s_nodes_available(only_name=False)
        cloud_nodes = self.get_k8s_cloud_nodes()
        cloud_node_names = [x.metadata.name for x in cloud_nodes]

        my_nodes = dict([(n.metadata.name, Node(name=n.metadata.name, k8s_node=n,
                                                memory=self.convert_to_int(n.status.allocatable['memory'])))
                         if n.metadata.name not in cloud_node_names else
                         (n.metadata.name, Node(name=n.metadata.name, k8s_node=n,
                                                memory=self.convert_to_int(n.status.allocatable['memory']), cloud=True))
                         for n in nodes])
        return my_nodes

    def optimize(self, retry=0):
        """Executing the OfflineOrchestrator for the opitmization of the provisioned placeholder resources

        Args:
            retry (int): The actual number of retrials

        Returns:
            (int, int, int, int): Returns with the optimization duration,
                the total placeholder size after the optimization, the total placeholder size before the optimization,
                the number of used Pods
        """
        if not retry:
            retry = 0
        print('Generating input for optimization')
        # 1. Collect the node information from the system
        new_pods = {}
        for p in self.pods:
            pod = self.pods[p]
            new_pods[pod.name] = Pod(name=pod.name, node=None, k8s_pod=None, memory_request=pod.memory_request,
                                     constraint=pod.constraint)
        new_nodes = {}
        for n in self.nodes:
            node = self.nodes[n]
            new_nodes[node.name] = Node(name=node.name, k8s_node=None, memory=node.get_max_memory(), cloud=node.cloud)
        if custom_config.CLUSTERING:
            offline_orchestrator = OfflineOrchestrator(nodes=new_nodes, pods=new_pods, delay_matrix=self.delay_matrix,
                                                       metadata=self._dm_metadata, clusterer=self.clusterer)
        else:
            offline_orchestrator = OfflineOrchestrator(nodes=new_nodes, pods=new_pods, delay_matrix=self.delay_matrix,
                                                       metadata=self._dm_metadata)
        print('Start optimization')
        optimization_start = time.time()
        optimize_ok = offline_orchestrator.run()
        if optimize_ok:
            optimization_end = time.time()
            online_placeholders_size = sum([self.placeholders[x].required_memory for x in self.placeholders])
            offline_placeholders_size = sum([offline_orchestrator.placeholders[x].required_memory
                                             for x in offline_orchestrator.placeholders])
            print('Optimization ended')
            dur = optimization_end-optimization_start
            if offline_placeholders_size < online_placeholders_size:
                self.patch_optimization_results(offline_orchestrator)
            return dur, offline_placeholders_size, online_placeholders_size, len(new_pods)
        else:
            if retry < 20:
                try:
                    offline_orchestrator.validate_result()
                except Exception:
                    retry += 1
                    self.optimize(retry=retry)
            else:
                offline_orchestrator.validate_result()

    def patch_optimization_results(self, offline_orchestrator):
        """Applying the changes made by the OfflineOrchestrator, to reduce the amount of placeholder size

        Args:
            offline_orchestrator (offline_orchestrator.OfflineOrchestrator):

        Returns:

        """
        # TODO: Implement
        pass

    def check_all_constraints(self):
        """Checking all Pods' constraint if it still fulfilled
            If a constraint violation is found, we try to fix that
        """
        print("Checking all the constraints in the system")
        for pod_name, pod in self.pods.items():
            if pod.constraint != {} and pod.name not in self.reschedules:
                previous_element, delay = list(pod.constraint.items())[0]
                nodes_in_radius = self.get_nodes_in_radius(previous_element, delay)
                if pod.node not in nodes_in_radius and pod.node in self._dm_metadata:
                    print("Constraint violation for {}".format(pod.name))
                    print("Constraint: {}".format(str(pod.constraint)))
                    placeholder = self.get_placeholder_by_pod(pod)
                    placeholder_mem_matrix = self.get_memory_matrix(placeholder, nodes_in_radius)
                    for node in nodes_in_radius:
                        if placeholder is not None and node == placeholder.node and len(placeholder.pods) == 1:
                            self.patch_pod(pod, node)
                            break
                        elif placeholder is not None and node != placeholder.node \
                                and nodes_in_radius[node].available_memory >= pod.memory_request \
                                and placeholder_mem_matrix[node]+pod.memory_request <= placeholder.required_memory:
                            self.patch_pod(pod, node)
                            break
                    # TODO: Other options
        print("Constraint check finished successfully")

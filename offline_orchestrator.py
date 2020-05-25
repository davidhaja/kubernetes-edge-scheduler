from baseclasses.placeholder import Placeholder
from baseclasses.pod import Pod
from baseclasses.node import Node
from baseclasses.scheduler import BaseScheduler
import copy
import configs.config as myconfig
import numpy as np


class OfflineOrchestrator(BaseScheduler):
    """OfflineOrchestrator class that supports placeholder size optimization
       with the aware of network latency scheduling

    """

    def __init__(self, nodes, pods, delay_matrix, metadata, clusterer=None):
        """Constructor of OfflineOrchestrator

        Args:
            nodes (dict[str, baseclasses.node.Node]): Dictionary that contains the node name: Node items
            pods (dict[str, baseclasses.pod.Pod]): Dictionary that contains the Pod name: Pod items
            delay_matrix (numpy.ndarray): The matrix that holds the latency values between nodes
            metadata (dict[str, int]): Since numpy.ndarray cannot contain labels, we store the indexes of the nodes
            clusterer (baseclasses.cluster.DynamicClusterer): Clusterer object that constains the clusters
        """
        super(OfflineOrchestrator, self).__init__(delay_matrix=delay_matrix, metadata=metadata, nodes=nodes, pods=pods)
        self.placeholders = {}
        self.clusterer = clusterer

    def get_nodes_in_radius(self, previous_element_name, required_delay):
        """Get the nodes that are in the radius of the previous element with the given delay value
        If clustering enabled, we get the cluster/nodes from the baseclasses.cluster.DynamicClusterer object.

        Args:
            previous_element_name (str): The name of the previous element. Can be Pod's name or node's name
            required_delay (int): The delay value that gives the maximum distance from the previous element

        Returns:
            (dict[str, baseclasses.node.Node]): Dictionary that contains the nodes in the radius of the previous element
        """
        if not myconfig.CLUSTERING:
            return self.get_nodes_in_radius_without_clustering(previous_element_name, required_delay)
        else:
            previous_node = self.get_node_from_podname_or_nodename(previous_element_name)
            nodes_in_rad_from_clusterer = self.clusterer.get_nodes_in_radius(previous_node.name, required_delay)
            nodes_in_radius = dict()
            for n in nodes_in_rad_from_clusterer:
                node = self.get_node_from_name(n)
                try:
                    if node.alive:
                        nodes_in_radius[node.name] = node
                except AttributeError:
                    pass
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
            try:
                if node.alive:
                    nodes_in_radius[node.name] = node
            except AttributeError:
                pass
        return nodes_in_radius

    def run(self):
        """The core method of the optimization
            First, it deploys the palceholders with minimum size.
            Second, it deploys the Pods.
            Third, it checks and repairs anomalies in the system.

        Returns:
            (bool): Returns whether there are any anomalies in the system
        """
        self.deploy_placeholders()

        self.deploy_pods()

        good, bad_placeholders = self.check_placeholder_size_match_from_nodes()
        if not good:
            ok = self.repair(bad_placeholders)
            return ok
        return good

    def repair(self, bad_placeholders):
        """Repair phase of bad placeholders

        Args:
            bad_placeholders (dict[str, ]): The placeholders those size are insufficient

        Returns:
            (bool): Returns whether any bad placeholder left (True -> no bad placeholder left)
        """
        # This for loop iterates through the placeholders which doesn't fulfill the size requirements
        fixed = []
        for p_id in bad_placeholders.keys():
            # First we, try to backup the pod somewhere else
            reassign_success = self.try_fix_placeholder_with_reassign(p_id, bad_placeholders[p_id])
            if reassign_success:
                fixed.append(p_id)
        for f in fixed:
            del bad_placeholders[f]
        if len(bad_placeholders.keys()) == 0:
            return True

        fixed = []
        for p_id in bad_placeholders.keys():
            increase_success = self.try_fix_placeholders_with_increase(p_id, bad_placeholders[p_id])
            if increase_success:
                fixed.append(p_id)
        for f in fixed:
            del bad_placeholders[f]
        if len(bad_placeholders.keys()) == 0:
            return True

        # Try once again to repair
        return self.repair(bad_placeholders)

    def try_fix_placeholders_with_new_placeholder(self, placeh_id, bad_nodes_dict):
        """Try to fix the bad placeholder with creating new placeholder on other nodes

        Args:
            placeh_id (str): Bad placeholder id
            bad_nodes_dict (dict[str, int]): Node name: required memory against the given placeholder items in the dict

        Returns:
            (bool): Whether the fix is successful or not
        """
        bad_placeholder = self.placeholders[placeh_id]
        bad_placeholder_node = self.nodes[bad_placeholder.node]
        keys = bad_nodes_dict.keys()
        # Iterate through all pod ids in the bad placeholder (which are covered with this placeholder)
        for pod_id in copy.copy(bad_placeholder.pods):
            # We try to change the pod's placeholder_id to another node's placeholder
            pod = self.pods[pod_id]
            pod_placed_node = self.nodes[pod.node]
            if pod.node in keys:
                # If placeholder has less reserved memory than the sum of covered actual pod's node
                # So basically P_n^m < P^m not fulfilled,
                # where n is the actual pod's node and P^m is the bad placeholder
                if not self.check_placeholder_memory_size_against_specified_node(bad_placeholder, pod_placed_node, 0):
                    previous_element_name = list(pod.constraint.keys())[0]
                    delay = pod.constraint[previous_element_name]
                    # Getting the nodes which in pod's radius
                    nodes_in_pod_rad = self.get_nodes_in_radius(previous_element_name, delay)
                    for n in nodes_in_pod_rad:
                        if self.nodes[n] != bad_placeholder_node and self.nodes[n].name != pod.node and \
                                self.nodes[n].placeholder is None and \
                                self.nodes[n].available_memory >= pod.memory_request and n not in keys:
                            # Create new placeholder object
                            placeholder = Placeholder(node=n, required_memory=pod.memory_request)
                            # Add placeholder object to orchestrator's placeholders list
                            self.placeholders[placeholder.id] = placeholder
                            # Add placeholder to the i-th node from the sorted list
                            self.nodes[n].add_placeholder(placeholder)
                            # Assign the i-th node's placable pod ids to placeholder object pod_ids variable
                            placeholder.pods.add(pod.name)
                            # Delete pod id from old placeholder
                            bad_placeholder.pods.remove(pod.name)
                            bad_nodes_dict[pod_placed_node.name] -= pod.memory_request
                            break
                    if not any(bad_nodes_dict[x] != 0 for x in keys):
                        return True
        return False

    def try_fix_placeholder_with_reassign(self, placeh_id, bad_nodes_dict):
        """Try to fix the bad placeholder with reassigning Pod to another placeholder or moving the Pod to another node

        Args:
            placeh_id (str): Id of the placeholder
            bad_nodes_dict (dict[str, int]): Node name: required memory against the given placeholder items in the dict

        Returns:
            (bool): Whether the fix is successful or not
        """
        bad_placeholder = self.placeholders[placeh_id]
        bad_placeholder_node = self.nodes[bad_placeholder.node]
        keys = bad_nodes_dict.keys()
        # Iterate through all pod ids in the bad placeholder (which are covered with this placeholder)
        for pod_id in copy.copy(bad_placeholder.pods):
            # We try to change the pod's placeholder_id to another node's placeholder
            pod = self.pods[pod_id]
            pod_placed_node = self.nodes[pod.node]
            if pod_placed_node.name in keys and bad_nodes_dict[pod_placed_node.name] > 0:
                # If placeholder has less reserved memory than the sum of covered actual pod's node
                # So basically P_n^m < P^m not fulfilled,
                # where n is the actual pod's node and P^m is the bad placeholder
                previous_element_name = list(pod.constraint.keys())[0]
                delay = pod.constraint[previous_element_name]
                # Getting the nodes which in pod's radius
                nodes_in_pod_rad = self.get_nodes_in_radius(previous_element_name, delay)
                for n in nodes_in_pod_rad:
                    # bad placeholder's node, pod's node, node without placeholder does NOT count!
                    if self.nodes[n] != bad_placeholder_node and self.nodes[n].name != pod.node and \
                            self.nodes[n].placeholder is not None and n not in keys:
                        maybe_new_placeholder = self.nodes[n].placeholder
                        new_placeholder_ok = self.check_placeholder_memory_size_against_specified_node(
                            maybe_new_placeholder, pod_placed_node, pod.memory_request)
                        if new_placeholder_ok:
                            # Assign pod to new placeholder
                            maybe_new_placeholder.pods.add(pod.name)
                            # Delete pod id from old placeholder
                            bad_placeholder.pods.remove(pod.name)
                            bad_nodes_dict[pod_placed_node.name] -= pod.memory_request
                            break
                    if self.nodes[n] != bad_placeholder_node and self.nodes[n].name != pod.node and \
                       self.nodes[n].available_memory >= pod.memory_request:
                        # Try to move pod to another node
                        maybe_node_ok = self.check_placeholder_memory_size_against_specified_node(bad_placeholder,
                                                                                                  self.nodes[n],
                                                                                                  pod.memory_request)
                        if maybe_node_ok:
                            bad_nodes_dict[pod.node] -= pod.memory_request
                            self.nodes[pod.node].delete_mypod(pod)
                            self.nodes[n].add_mypod(pod)
                            pod.node = self.nodes[n].name
                            break
                if not any(bad_nodes_dict[x] != 0 for x in keys):
                    return True
        return False

    def try_fix_placeholders_with_increase(self, placeh_id, bad_p_dict):
        """Try to fix the bad placeholder with increasing placeholder's size

        Args:
            placeh_id (str): Id of the placeholder
            bad_p_dict (dict[str, int]): Node name: required memory against the given placeholder items in the dict

        Returns:
            (bool): Whether the fix is successful or not
        """
        bad_placeholder = self.placeholders[placeh_id]
        required_size = max([bad_p_dict[x] for x in bad_p_dict.keys()])
        node = self.nodes[bad_placeholder.node]
        if node.available_memory >= required_size - bad_placeholder.required_memory:
            # If placeholder's node has enough free memory, just increase the placeholder's size
            # print('Placeholder {} required size: {} ; '\
            #       'current size: {} ; node name: {} ; available memory: {}'.format(bad_placeholder,
            #                                                                        str(required_size),
            #                                                                        str(bad_placeholder.required_memory),
            #                                                                        node.name,
            #                                                                        str(node.available_memory)))
            self.modify_placeholder_size(bad_placeholder, required_size - bad_placeholder.required_memory)
            return True
        return False

    def check_placeholder_memory_size_against_specified_node(self, placeholder, node, required_extra_memory):
        """Checking if the placeholder's size is sufficient against the given node plus the extra memory

        Args:
            placeholder (baseclasses.placeholder.Placeholder): Placeholder object we examine
            node (baseclasses.node.Node): Node object we examine
            required_extra_memory (int): Extra memory requirement

        Returns:
            (bool): Whether the placeholder's actual size is sufficient
        """
        required_size = required_extra_memory
        for pod in node.pods:
            if self.pods[pod].name in placeholder.pods:
                required_size += self.pods[pod].memory_request
        if required_size > placeholder.required_memory:
            return False
        else:
            return True

    def check_placeholder_memory_size_against_all_nodes(self, placeholder, required_extra_memory):
        """Checking if the placeholder's size is sufficient against all nodes with extra memory requirement

        Args:
            placeholder (baseclasses.placeholder.Placeholder): Placeholder object we examine
            required_extra_memory (int): Extra memory requirement

        Returns:
            (bool): Whether the placeholder's actual size is sufficient
        """
        for node in self.nodes:
            required_size = required_extra_memory
            for pod in self.nodes[node].pods:
                if pod.name in placeholder.pods:
                    required_size += pod.memory_request
            if required_size > placeholder.required_memory:
                return False
        return True

    def modify_placeholder_size(self, placeholder, size):
        """Modifying the given placeholder's memory request

        Args:
            placeholder (baseclasses.placeholder.Placeholder): The placeholder that is modified
            size (int): The extra memory requirement

        """
        placeholder.required_memory += size
        node = self.nodes[placeholder.node]
        node.decrease_available_memory(size)

    def deploy_placeholders(self):
        """Placeholder deployment phase

        """
        pod_set = set(self.pods.keys())
        sorted_nodes = self.sort_nodes_by_number_of_placable_pods()
        placeholded_pod_ids = []
        i = 0
        while len(pod_set) > 0:
            node = self.nodes[sorted_nodes[i]]
            # Create new placeholder object
            placeholder = Placeholder(node=node.name,
                                      required_memory=self.get_min_ram_request_from_pod_ids(node.placeable_pods))
            # Add placeholder object to orchestrator's placeholders list
            self.placeholders[placeholder.id] = placeholder
            # Add placeholder to the i-th node from the sorted list
            node.add_placeholder(placeholder)
            # Assign the i-th node's placable pod ids to placeholder object pod_ids variable
            placeholder.pods = node.placeable_pods
            # Append the i-th node's placeable pod ids to placeholded_pod_ids list
            # In general: mark the pods as already placeholded
            placeholded_pod_ids += node.placeable_pods
            # Remove the newly placeholded pod ids form pod_set
            pod_set = pod_set.difference(node.placeable_pods)
            # In this for cycle we delete the pod ids from each node's placeable_pods set
            # This is because we reorder the sorted_nodes list but without the already placeholded pods
            for pd in node.placeable_pods:
                # Set the placeholder_id for each pod assigned with this exact placeholder
                self.pods[pd].placeholder_id = placeholder.id
                for n in sorted_nodes:
                    if pd in self.nodes[n].placeable_pods and n != sorted_nodes[i]:
                        self.nodes[n].placeable_pods.remove(pd)
            # delete the pod ids from the chosen node's placeable_pods set -> it is gonna be an empty set
            node.placeable_pods = set()
            sorted_nodes = sorted(sorted_nodes, key=lambda x: len(self.nodes[x].placeable_pods), reverse=True)
            # print('{} placed on: {} ; Placeholded pods: {}'.format(placeholder, placeholder.node,
            #                                                        str(placeholder.pods)))
        print('Placeholders placed')

    def deploy_pods(self):
        """Pod deployment phase

        """
        sorted_pod_list, pod_deploy_helper = self.sort_pods_by_number_of_nodes()
        for pod_id in sorted_pod_list:
            pod_dict = pod_deploy_helper[pod_id]
            pod = self.pods[pod_id]
            node_dict = dict()
            for x in pod_dict:
                if self.nodes[x].available_memory >= pod.memory_request and self.nodes[x].placeholder \
                        and pod.name not in self.nodes[x].placeholder.pods:
                    node_dict[x] = self.get_node_from_name(x)
                elif self.nodes[x].available_memory >= pod.memory_request and self.nodes[x].placeholder is None:
                    node_dict[x] = self.get_node_from_name(x)
            # print('Pod: {} ; Valid nodes: {}'.format(pod.name, str([self.nodes[x].name for x in node_dict])))
            if len(node_dict) == 0:
                raise Exception('Unable to deploy pods')
            chosen_node = self.choose_min_utilized_by_num_of_pods(node_dict)
            chosen_node.add_mypod(pod)
            pod.node = chosen_node.name
            # print('Pod: {} ; Chosen node: {} '.format(pod.name, chosen_node.name))

    def sort_pods_by_number_of_nodes(self):
        """Sorting the pods by the number of possible nodes in their radius

        Returns:
            (list[str], dict[str, dict[str, node]]):
                Returns with a tuple. The first item is the ordered list of keys of the second item,
                which is a dictionary. The dictionary contains the pods and the belonging nodes in their radius
        """
        pod_deploy_helper = {}
        for pod in self.pods:
            start = list(self.pods[pod].constraint.keys())[0]
            delay = self.pods[pod].constraint[start]
            pod_deploy_helper[pod] = self.get_nodes_in_radius(start, delay)
        return sorted(pod_deploy_helper, key=lambda x: len(pod_deploy_helper[x])), pod_deploy_helper

    def sort_nodes_by_number_of_placable_pods(self, nodes=None, services=None):
        """Sorting the nodes by the number of possible Pods that could be deployed on them

        Args:
            nodes (dict[str, baseclasses.node.Node]): Dictionary of nodes
            services (dict[str, baseclasses.pod.Pod]): Dictionary of Pods

        Returns:
            (list[str]): Returns the sorted list of node names
        """
        if not nodes:
            nodes = self.nodes
        if not services:
            services = self.pods

        for service in services:
            start = list(services[service].constraint.keys())[0]
            delay = services[service].constraint[start]
            nodes_in_radius = self.get_nodes_in_radius(start, delay)
            for n in nodes_in_radius:
                nodes_in_radius[n].placeable_pods.add(services[service].name)

        return sorted(nodes, key=lambda x: len(nodes[x].placeable_pods), reverse=True)

    def get_min_ram_request_from_pod_ids(self, pod_names):
        """Get the minimum request for the placeholder, which is equals with the max memory request from the pods

        Args:
            pod_names (list[str]): List of Pod names

        Returns:
            (int): The minimum placeholder size
        """
        min_ram = 0
        for pid in pod_names:
            if min_ram < self.pods[pid].memory_request:
                min_ram = self.pods[pid].memory_request
        return min_ram

    def check_placeholder_size_match_from_nodes(self):
        """Check if all placeholders have sufficient amount of provisioned resource against all nodes

        Returns:
            (bool, dict[str, dict[str, int]]):
                (True, None) if all placeholders have enough provisioned resource against all nodes
                (False, dict) return value contains the bad placeholders and the nodes that belong under their cover
        """
        helping_dict = {}
        bad_placeholders = {}
        for n in self.nodes:
            node = self.nodes[n]
            helping_dict[node.name] = {}
            for m in self.nodes:
                # n node's pods placeholded on m
                helping_dict[node.name][self.nodes[m].name] = 0
        for n in self.nodes:
            for pod_id in self.nodes[n].pods:
                placeholder = self.get_placeholder_by_pod(pod_id)
                helping_dict[self.nodes[n].name][placeholder.node] += self.pods[pod_id].memory_request
        for ph_k in self.placeholders:
            placeholder = self.placeholders[ph_k]
            for x in helping_dict.keys():
                if placeholder.required_memory < helping_dict[x][placeholder.node]:
                    if placeholder.id not in bad_placeholders.keys():
                        bad_placeholders[placeholder.id] = dict()
                    bad_placeholders[placeholder.id][x] = helping_dict[x][placeholder.node]

        if len(bad_placeholders.keys()) > 0:
            return False, bad_placeholders
        return True, None

    def validate_result(self):
        """Validation of the optimization result

        Returns:
            (bool): Whether the result is valid (no anomaly left)
        """
        nodes = self.nodes
        service_pods = self.pods
        replicated_pods = []
        for placeholder in self.placeholders:
            replicated_pods += self.placeholders[placeholder].pods
        if any(x not in replicated_pods for x in self.pods):
            raise Exception('Pod not covered with replication!')

        for pod in service_pods:
            if self.pods[pod].node == self.placeholders[self.pods[pod].placeholder_id].node:
                raise Exception('Pod on the same node as its placeholder!')

        bad_placeholders = self.check_placeholder_size_match_from_nodes()
        # print(str(bad_placeholders))

        helping_dict = {}
        for n in nodes:
            helping_dict[n] = {}
            for m in nodes:
                # n node's pods placeholded on m
                helping_dict[n][m] = 0
        for n in nodes:
            for pod_id in self.nodes[n].pods:
                placeholder = self.placeholders[self.pods[pod_id].placeholder_id]
                helping_dict[n][placeholder.node] += self.pods[pod_id].memory_request
        for placeholder in self.placeholders:
            required_size = max([helping_dict[x][self.placeholders[placeholder].node] for x in helping_dict.keys()])
            if self.placeholders[placeholder].required_memory < required_size:
                raise Exception('P_m^n < P_m not fulfilled '
                                'Placeholder {} '
                                'required size: {} '
                                'reserved size: {}'.format(placeholder, str(required_size),
                                                           str(self.placeholders[placeholder].required_memory)))
        return True

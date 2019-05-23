#!/usr/bin/env python3

import time
import random
import json
from kubernetes.client.rest import ApiException
from kubernetes import client, config
from placeholder import Placeholder


class CustomScheduler(object):

    def __init__(self, scheduler_name="custom-scheduler"):
        self.load_config()
        self.v1 = client.CoreV1Api()
        self.scheduler_name = scheduler_name
        self.placeholders = []
        self.rescedules = dict()

    @staticmethod
    def load_config():
        try:
            config.load_kube_config()
        except FileNotFoundError as e:
            print("WARNING %s\n" % e)
            config.load_incluster_config()

    def nodes_available(self):
        ready_nodes = []
        for n in self.v1.list_node().items:
            for status in n.status.conditions:
                if status.status == "True" and status.type == "Ready":
                    ready_nodes.append(n.metadata.name)
        return ready_nodes

    def get_all_pods(self, kube_system=False):
        if not kube_system:
            return [x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if
                    x.metadata.namespace != 'kube-system']
        else:
            return self.v1.list_pod_for_all_namespaces(watch=False).items

    def get_pod_by_name(self, name):
        return next(x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if name in x.metadata.name)

    def get_pods_on_node(self, node_name, kube_system=False):
        if not kube_system:
            return [x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if
                    x.metadata.namespace != 'kube-system' and x.spec.node_name == node_name]
        else:
            return [x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if x.spec.node_name == node_name]

    @staticmethod
    def convert_to_int(resource_string):
        if 'Ki' in resource_string:
            return int(resource_string.split('K')[0])*1024
        elif 'Mi' in resource_string:
            return int(resource_string.split('M')[0])*(1024**2)
        elif 'Gi' in resource_string:
            return int(resource_string.split('G')[0])*(1024**3)

    def calculate_available_memory(self, node):
        pods_on_node = self.get_pods_on_node(node.metadata.name)
        sum_reserved_memory = sum([self.get_pod_memory_request(y) for y in pods_on_node])
        allocatable_memory = self.convert_to_int(node.status.allocatable['memory'])
        try:
            sum_reserved_memory += next(x.required_memory for x in self.placeholders if x.node == node.metadata.name)
        except StopIteration:
            pass
        return allocatable_memory - sum_reserved_memory

    def get_cloud_node(self):
        return next(x for x in self.v1.list_node().items if
                    'kubernetes.io/role' in x.metadata.labels.keys() and
                    x.metadata.labels['kubernetes.io/role'] == 'cloud')

    def get_node_from_name(self, node_name):
        return next(x for x in self.v1.list_node().items if x.metadata.name == node_name)

    def get_node_from_podname_or_nodename(self, previous_element_name):
        if previous_element_name in [x.metadata.name for x in self.v1.list_node().items]:
            return self.get_node_from_name(previous_element_name)
        else:
            return self.get_node_from_name(next(x for x in self.v1.list_pod_for_all_namespaces(watch=False).items if
                                                previous_element_name in x.metadata.name).spec.node_name)

    def get_nodes_in_radius(self, previous_element_name, required_delay):
        available_nodes = self.nodes_available()
        previous_node = self.get_node_from_podname_or_nodename(previous_element_name)
        # FIXME: Now we assume that rtt i->j is the same as rtt j->i
        rtts = [int(x.split('-')[1]) for x in previous_node.metadata.labels.keys() if 'rtt-' in x]
        node_names = set()
        node_names.add(previous_node.metadata.name)
        for rtt in rtts:
            if rtt <= required_delay:
                node_names = node_names.union(set(previous_node.metadata.labels['rtt-'+str(rtt)].split('_')))
        return [self.get_node_from_name(x)for x in node_names if x in available_nodes]

    def reused_placeholder_unused_pod_node(self, placeholder, nodes_enough_resource):
        covered_nodes = [self.get_node_from_podname_or_nodename(x) for x in placeholder.pods]
        covered_node_names = [y.metadata.name for y in covered_nodes]
        if any(x.metadata.name not in covered_node_names+[placeholder.node] for x in nodes_enough_resource):
            return True, covered_node_names+[placeholder.node]
        return False, None

    def get_memory_matrix(self, placeholder, nodes_enough_resource):
        placeholder_memory_matrix = {}
        for n in nodes_enough_resource:
            if n.metadata.name != placeholder.node:
                placeholder_memory_matrix[n.metadata.name] = 0
                for pod in self.get_pods_on_node(n.metadata.name):
                    if pod.metadata.name.split('-')[0] in placeholder.pods:
                        placeholder_memory_matrix[n.metadata.name] += self.get_pod_memory_request(pod)
        return placeholder_memory_matrix

    def reused_placeholder_used_pod_node(self, placeholder, pod, nodes_enough_resource):
        placeholder_memory_matrix = self.get_memory_matrix(placeholder, nodes_enough_resource)
        if any(placeholder_memory_matrix[x] + self.get_pod_memory_request(pod) <= placeholder.required_memory
               for x in placeholder_memory_matrix.keys()):
            return True, next(x for x in nodes_enough_resource if x.metadata.name in placeholder_memory_matrix.keys()
                              and placeholder_memory_matrix[x.metadata.name] +
                              self.get_pod_memory_request(pod) <= placeholder.required_memory)
        return False, None

    def create_new_placeholder(self, nodes_enough_resource):
        placeholder = Placeholder()
        # placeholder.node = random.choice(nodes_enough_resource).metadata.name
        placeholder.node = nodes_enough_resource[-1].metadata.name
        self.placeholders.append(placeholder)
        return placeholder

    def add_pod_to_placeholder(self, pod, placeholder, extra_memory=0):
        placeholder.pods.add(pod.metadata.name.split('-')[0])
        placeholder.required_memory += extra_memory

    def narrow_placeholders_in_rad(self, node_names_in_rad):
        placeholders_in_rad = []
        for placeholder in self.placeholders:
            if placeholder.node in node_names_in_rad:
                placeholders_in_rad.append(placeholder)
        return placeholders_in_rad

    def assign_placeholder(self, pod, nodes_less_resource, nodes_enough_resource):
        # len(nodes_enough_resource) + len(nodes_less_resource) is always greater than 1!
        node_names_in_rad = [x.metadata.name for x in nodes_less_resource]
        node_names_in_rad += [x.metadata.name for x in nodes_enough_resource]
        placeholders_in_rad = self.narrow_placeholders_in_rad(node_names_in_rad)

        for placeholder in placeholders_in_rad:
            is_any_usable, excluded_list = self.reused_placeholder_unused_pod_node(placeholder, nodes_enough_resource)
            if is_any_usable:
                self.add_pod_to_placeholder(pod, placeholder)
                return [x.metadata.name not in excluded_list for x in nodes_enough_resource]

        for placeholder in placeholders_in_rad:
            is_any_usable, chosen_node = self.reused_placeholder_used_pod_node(placeholder, pod, nodes_enough_resource)
            if is_any_usable:
                self.add_pod_to_placeholder(pod, placeholder)
                return [chosen_node]

        # TODO: Another option, when we have to increase the placeholder's size
        #  for assigning the pod somewhere in the radius

        if len(nodes_enough_resource) > 1:
            placeholder = self.create_new_placeholder(nodes_enough_resource)
            self.add_pod_to_placeholder(pod, placeholder, self.get_pod_memory_request(pod))
            return [x for x in nodes_enough_resource if x.metadata.name != placeholder.node]
        else:
            print("WARNING Can not create placeholder for this pod!")
            return nodes_enough_resource

    def pod_has_placeholder(self, pod):
        try:
            # FIXME: '-' character assumed as splitting character
            return True, next(ph for ph in self.placeholders if pod.metadata.name.split('-')[0] in ph.pods)
        except StopIteration:
            return False, None

    def get_pod_memory_request(self, pod):
        return sum([self.convert_to_int(x.resources.requests['memory']) for x in pod.spec.containers if
                    x.resources.requests is not None])

    def narrow_nodes_by_capacity(self, pod, node_list):
        return_list = []
        for n in node_list:
            if self.calculate_available_memory(n) > self.get_pod_memory_request(pod):
                return_list.append(n)
        return return_list

    def get_placeholder_by_pod(self, pod):
        try:
            return next(x for x in self.placeholders if pod.metadata.name.split('-')[0] in x.pods)
        except StopIteration:
            return None

    def get_reschedulable(self, node, new_memory_request):
        pods_on_node = self.get_pods_on_node(node.metadata.name)
        for old_pod in pods_on_node:
            old_memory_request = self.get_pod_memory_request(old_pod)
            if old_memory_request >= new_memory_request:
                old_start_point = next(x for x in old_pod.metadata.labels.keys() if 'delay_' in x).split('_')[1]
                old_required_delay = int(old_pod.metadata.labels['delay_' + old_start_point])
                old_nodes_in_radius = self.narrow_nodes_by_capacity(old_pod,
                                                                    self.get_nodes_in_radius(old_start_point,
                                                                                             old_required_delay))
                old_placeholder = self.get_placeholder_by_pod(old_pod)
                if len([x for x in old_nodes_in_radius if x.metadata.name != old_placeholder.node]) > 0:
                    return True, old_pod, \
                           random.choice([x for x in old_nodes_in_radius if x.metadata.name != old_placeholder.node])
        return False, None, None

    def reschedule_pod(self, new_pod, new_nodes_in_radius):
        new_memory_request = self.get_pod_memory_request(new_pod)
        for n in new_nodes_in_radius:
            any_reschedulable, old_pod, reschedule_node = self.get_reschedulable(n, new_memory_request)
            if any_reschedulable:
                self.do_reschedule(old_pod, reschedule_node)
                return old_pod.metadata.name
        return None

    def do_reschedule(self, old_pod, reschedule_node):
        self.patch_pod(old_pod, reschedule_node.metadata.name)

    def schedule(self, pod, namespace="default"):
        try:
            # FIXME: '-' character assumed as splitting character
            if pod.metadata.name.split('-')[0] in self.rescedules.keys():
                node = self.rescedules[pod.metadata.name.split('-')[0]]
                del self.rescedules[pod.metadata.name.split('-')[0]]
                self.bind(pod, node)
                return
            # Is there any placeholder assigned to this pod name?
            any_assigned_placeholder, placeholder = self.pod_has_placeholder(pod)
            if any_assigned_placeholder:
                # The Pod has already an assigned placeholder so probably a node failure occurred,
                # we need to restart the pod
                self.patch_pod(pod, placeholder.node)
            else:
                # New Pod request
                # Get the previous element name from where the delay constraint defined
                previous_element_name = next(x for x in pod.metadata.labels.keys() if 'delay_' in x).split('_')[1]
                # Get the delay constraint value from the labels
                required_delay = int(pod.metadata.labels['delay_'+previous_element_name])
                # Getting all the nodes inside the delay radius
                all_nodes_in_radius = self.get_nodes_in_radius(previous_element_name, required_delay)
                nodes_enough_resource_in_rad = self.narrow_nodes_by_capacity(pod, all_nodes_in_radius)
                if len(nodes_enough_resource_in_rad) == 0:
                    # There is no node with available resource
                    # Try to reschedule some previously deployed Pod
                    old_pod_name = self.reschedule_pod(pod, all_nodes_in_radius)
                    # We have to wait, while the pod get successfully rescheduled
                    if old_pod_name is not None:
                        # FIXME: '-' character assumed as splitting character
                        # FIXME: We are waiting till only 1 instance remain. There can be more on purpose!
                        time.sleep(3)
                        print("INFO Waiting for rescheduling.")
                        while len([x.metadata.name for x in self.get_all_pods() if
                                   old_pod_name.split('-')[0] in x.metadata.name]) > 1:
                            time.sleep(1)
                            print("INFO Waiting for rescheduling.")
                    # Recalculate the nodes with the computational resources
                    nodes_enough_resource_in_rad = self.narrow_nodes_by_capacity(pod, all_nodes_in_radius)
                if len(all_nodes_in_radius) > 1:
                    nodes_enough_resource_in_rad = self.assign_placeholder(pod, [x for x in all_nodes_in_radius if
                                                                                 x not in nodes_enough_resource_in_rad],
                                                                           nodes_enough_resource_in_rad)
                    for ph in self.placeholders:
                        print("INFO Placeholder on node: %s ;assigned Pods: %s\n" % (ph.node, str(ph.pods)))
                elif len(all_nodes_in_radius) == 1:
                    print("WARNING No placeholder will be assigned to this Pod!")
                node = nodes_enough_resource_in_rad[0]
                self.bind(pod, node.metadata.name, namespace)
        except StopIteration:
            # No delay constraint for the pod
            node = self.get_cloud_node()
            self.bind(pod, node.metadata.name, namespace)

    def bind(self, pod, node, namespace="default"):
        target = client.V1ObjectReference(api_version='v1', kind='Node', name=node)
        meta = client.V1ObjectMeta()
        meta.name = pod.metadata.name

        body = client.V1Binding(target=target, metadata=meta)

        try:
            print("INFO Pod: %s placed on: %s\n" % (pod.metadata.name, node))
            api_response = self.v1.create_namespaced_pod_binding(name=pod.metadata.name, namespace=namespace, body=body)
            print(api_response)
            return api_response
        except Exception as e:
            print("Warning when calling CoreV1Api->create_namespaced_pod_binding: %s\n" % e)

    def patch_deployment(self, pod, node, namespace="default"):
        extensions_v1beta1 = client.ExtensionsV1beta1Api()
        deployment_name = pod.metadata.name.split('-')[0]
        deployment = [x for x in extensions_v1beta1.list_deployment_for_all_namespaces(watch=False).items if
                          x.metadata.namespace == 'default' and x.metadata.name == deployment_name][-1]
        deployment.spec.template.spec.node_name = node
        api_response = extensions_v1beta1.replace_namespaced_deployment(name=deployment_name, namespace=namespace,
                                                                        body=deployment)

    def patch_pod(self, pod, node, namespace="default"):
        # FIXME: '-' character assumed as splitting character
        self.rescedules[pod.metadata.name.split('-')[0]] = node
        self.v1.delete_namespaced_pod(name=pod.metadata.name, namespace=namespace)


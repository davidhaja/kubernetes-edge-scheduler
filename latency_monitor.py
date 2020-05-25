#!/usr/bin/env python3
from kubernetes import client, config
from kubernetes.client.apis import core_v1_api
from kubernetes.stream import stream
import time
import itertools
import numpy as np
from configs import config as custom_config
import requests
import json


class LatencyMonitor(object):
    """Latency monitoring class

    """

    def __init__(self):
        """Constructor of LatencyMonitor

        """
        try:
            config.load_kube_config()
        except FileNotFoundError as e:
            print("WARNING %s\n" % e)
            config.load_incluster_config()
        self.api = core_v1_api.CoreV1Api()

    def get_worker_node_names(self):
        """Get the name of each node that are in Ready status

        Returns:
            (list[str]): The name of each node which is in available status
        """
        ready_nodes = []
        for n in self.api.list_node(watch=False).items:
            for status in n.status.conditions:
                if status.status == "True" and status.type == "Ready" \
                        and custom_config.MASTER_NAME not in n.metadata.name:
                    ready_nodes.append(n.metadata.name)
        return ready_nodes

    def create_pod_template(self, pod_name, node_name):
        """Creating V1PodTemplateSpec object for deploying ping pod

        Args:
            pod_name (str): Name of the Pod
            node_name (str): Name of the hosting node

        Returns:
            (kubernetes.client.V1PodTemplateSpec): Returns the created object
        """
        # Configureate Pod template container
        container = client.V1Container(
            name=pod_name,
            image=custom_config.PING_POD_IMAGE,
            command=['/sbin/init'])

        # Create and configurate a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(name=pod_name),
            spec=client.V1PodSpec(containers=[container], node_selector={"kubernetes.io/hostname": node_name}))
        return template

    def deploy_rtt_deployment(self, pod_IPs, pod_node_mapping):
        """Deploy Ping Pods

        Args:
            pod_IPs (dict[str, str]): Pod name and Pod IP dictionary
            pod_node_mapping (dict[str, str]): Pod name and node name dictionary

        """
        for pod, pod_ip in pod_IPs.items():
            if pod_ip is None:
                template = self.create_pod_template(pod, pod_node_mapping[pod])
                body = client.V1Pod(metadata=template.metadata, spec=template.spec)
                api_response = self.api.create_namespaced_pod(custom_config.NAMESPACE, body)

    def check_rtt_deployment(self, ping_pods):
        """Checking Ping Pods if they are running

        Args:
            ping_pods (list[str]): List of Ping Pod names

        """
        for pod in ping_pods:
            running = False
            time_out = 120
            cur_time = 0
            while cur_time < time_out:
                resp = self.api.read_namespaced_pod(name=pod, namespace=custom_config.NAMESPACE)
                if resp.status.phase == 'Running':
                    running = True
                    break
                time.sleep(1)
                cur_time += 1
            if not running:
                raise Exception("TIMEOUT: Pod {} is not running".format(pod))

    def get_ping_pod_IPs(self, ping_pods, pod_IPs):
        """Get the IP address of Ping Pods

        Args:
            ping_pods (list[str]): List of Ping Pod names
            pod_IPs (dict[str, str]): Pod name: Pod IP address dictionary

        Returns:
            (dict[str, str]): Pod name and Pod IP address mapping dictionary
        """
        ret = self.api.list_pod_for_all_namespaces(watch=False)
        for i in ret.items:
            if str(i.metadata.name) in ping_pods:
                pod_IPs[i.metadata.name] = i.status.pod_ip

        return pod_IPs

    def measure_rtt(self, pod_from, pod_to_IP):
        """Measure the latency (RTT) between two Pods

        Args:
            pod_from (str): Name of source Pod
            pod_to_IP (str): Destination Pod's IP address

        Returns:
            (float): Returns the average RTT
        """
        exec_command = ['/bin/sh', '-c', 'ping -c 5 {}'.format(pod_to_IP)]

        resp = stream(self.api.connect_get_namespaced_pod_exec, pod_from, custom_config.NAMESPACE,
                      command=exec_command,
                      stderr=True, stdin=False,
                      stdout=True, tty=False)

        rtt_line = next(line for line in resp.split('\n') if 'rtt min/avg/max/mdev' in line)
        min_rtt = rtt_line.split('/')[3]
        avg_rtt = rtt_line.split('/')[4]
        max_rtt = rtt_line.split('/')[5]
        return float(avg_rtt)

    def get_rtt_labels_of_node(self, pod, rtt_matrix, ping_pods, pod_nodes_map):
        """Get the RTT label matrix for the given Pod

        Args:
            pod (str): Actual Pod's name
            rtt_matrix (dict[tuple[str, str], float]): Dictionary with items of two Pod's name and the delay between them
            ping_pods (list[str]): List of Ping Pod names
            pod_nodes_map (dict[str, str]): Ping Pods' name and their hosting node name mapping

        Returns:
            (dict[str, list[str]): Returns the RTT label matrix for the actual Pod
        """
        # FIXME: Currently we handle RTTs in millisec
        rtt_dict = {}

        for i in ping_pods:
            if i != pod:
                rtt = rtt_matrix[(pod, i)]
                rtt_ms = int(round(rtt))
                try:
                    rtt_dict["rtt-{}".format(rtt_ms)].append(pod_nodes_map[i])
                except:
                    rtt_dict["rtt-{}".format(rtt_ms)] = [pod_nodes_map[i]]

        return rtt_dict

    def do_measuring(self, pod_IPs, ping_pods):
        """Measure latencies between all Ping Pods

        Args:
            pod_IPs (dict[str, str]): Pod names and Pod IP addresses
            ping_pods (list[str]): List of Ping Pod names

        Returns:
            (dict[tuple[str, str], float]): Returns the dictionary that gives two Pod name and the RTT between them
        """
        ping_pods_permutations = list(itertools.permutations(ping_pods, 2))
        rtt_matrix = {(i, j): np.inf for (i, j) in ping_pods_permutations}
        # TODO: Measure delays paralelly
        for i, j in ping_pods_permutations:
            if rtt_matrix[(i, j)] == np.inf:
                print("\tMeasuring {} <-> {}".format(i, j))
                rtt_matrix[(i, j)] = self.measure_rtt(i, pod_IPs[j])
                rtt_matrix[(j, i)] = rtt_matrix[(i, j)]
        return rtt_matrix

    def measurement(self):
        """Core measurement method. Deploying Ping Pods, measuring latencies, constructing delay matrix and metadata

        Returns:
            (numpy.matrix, dict[str, int]): Returns with the delay matrix and its metadata
        """
        nodes = self.get_worker_node_names()
        print("Ready nodes: {}".format(str(nodes)))
        ping_pod_list = ["ping-pod-{}".format(i) for i in nodes]
        pod_nodes_mapping = {ping_pod_list[i]: nodes[i] for i in range(len(ping_pod_list))}
        temp_pod_IPs = {ping_pod_list[i]: None for i in range(len(ping_pod_list))}
        pod_IPs = self.get_ping_pod_IPs(ping_pod_list, temp_pod_IPs)

        # Deploy latency measurement pods
        self.deploy_rtt_deployment(pod_IPs, pod_nodes_mapping)
        self.check_rtt_deployment(ping_pod_list)

        pod_IPs =self.get_ping_pod_IPs(ping_pod_list, pod_IPs)

        # Measure latency
        pod_rtt_matrix = self.do_measuring(pod_IPs, ping_pod_list)
        metadata = dict()
        list_of_lists = []
        # Do labeling
        for i in range(len(ping_pod_list)):
            pod = ping_pod_list[i]
            metadata[pod_nodes_mapping[pod]] = i

            labels = self.get_rtt_labels_of_node(pod, pod_rtt_matrix, ping_pod_list, pod_nodes_mapping)
            temp_list = []
            for j in range(len(ping_pod_list)):
                if i == j:
                    temp_list.append(0)
                else:
                    dest_node = pod_nodes_mapping[ping_pod_list[j]]
                    temp_list.append(next(int(x.split('-')[-1]) for x in labels if dest_node in labels[x]))
            list_of_lists.append(np.array(temp_list))
        dm = np.array(list_of_lists)
        return dm, metadata

    def measure_latency_with_ping_pods(self, scheduler):
        """Periodically get latency values from Ping Pods and notify scheduler

        Args:
            scheduler (scheduler.CustomScheduler): The CustomScheduler object we notify with the new latencies

        """
        timeout = custom_config.MEASUREMENT_TIME
        while True:
            if timeout > 0:
                timeout -= 1
            else:
                try:
                    rtt_matrix, metadata = self.measurement()
                    scheduler.update_delay_matrix(rtt_matrix, metadata)
                    print("Labeling finished")
                except Exception as e:
                    print(e)
                    print("Labeling failed")
                timeout = custom_config.MEASUREMENT_TIME
            time.sleep(1)

    def watch_delay_file(self, scheduler, dm_file_path, metadata_file_path):
        """Periodically get latency values from static file and notify scheduler

        Args:
            scheduler (scheduler.CustomScheduler): The CustomScheduler object we notify with the new latencies
            dm_file_path (str): Path of the delay matrix file
            metadata_file_path (str): Path of the metadata file

        """
        timeout = custom_config.MEASUREMENT_TIME
        while True:
            if timeout > 0:
                timeout -= 1
            else:
                try:
                    rtt_matrix = np.loadtxt(dm_file_path)
                    with open(metadata_file_path) as metadata_json:
                        metadata = json.load(metadata_json)
                    scheduler.update_delay_matrix(rtt_matrix, metadata)
                except Exception as e:
                    print(e)
                    print("Loading matrix from file failed")
                timeout = custom_config.MEASUREMENT_TIME
            time.sleep(1)

    def measure_latency_with_goldpinger(self, scheduler):
        """Periodically get latency values from Goldpinger and notify scheduler

        Args:
            scheduler (scheduler.CustomScheduler): The CustomScheduler object we notify with the new latencies

        """
        timeout = custom_config.MEASUREMENT_TIME
        while True:
            if timeout > 0:
                timeout -= 1
            else:
                try:
                    rtt_matrix, metadata = self.init_goldpinger()
                    scheduler.update_delay_matrix(rtt_matrix, metadata)
                except Exception as e:
                    print(e)
                timeout = custom_config.MEASUREMENT_TIME
            time.sleep(1)

    def init_goldpinger(self):
        """Get latency values from Goldpinger and construct the delay matrix and metadata from it

        Returns:
            (tuple[numpy.ndarray, dict[str, int]]): Returns the delay matrix and the necessary metadata
        """
        resp = requests.get('http://{}:{}/check_all'.format(custom_config.GOLDPINGER_IP,
                                                            custom_config.GOLDPINGER_PORT)).json()
        pods = self.api.list_namespaced_pod(namespace=custom_config.GOLDPINGER_NAMESPACE,
                                            label_selector='app='+custom_config.GOLDPINGER_APP_NAME).items
        pod_node_dict = {pod.metadata.name: pod.spec.node_name for pod in pods if
                        resp['responses'][pod.metadata.name]['OK'] and pod.spec.node_name != custom_config.MASTER_NAME}
        dict_keys = list(pod_node_dict.keys())
        metadata = {}
        list_of_lists = []
        for i in range(len(dict_keys)):
            node = pod_node_dict[dict_keys[i]]
            pod_name = dict_keys[i]
            if resp['responses'][pod_name]['OK']:
                metadata[node] = i
                temp_list = []
                for j in range(len(dict_keys)):
                    if i == j:
                        temp_list.append(0)
                    else:
                        result_dict = resp['responses'][pod_name]['response']['podResults'][dict_keys[j]]
                        temp_list.append(result_dict['response-time-ms'])
                list_of_lists.append(np.array(temp_list))
        dm = np.array(list_of_lists)
        # Since we assume that rtt i->j equals to rtt j->i, we transform the delay matrix with the max values
        dm = self.transform_dm(dm)
        return dm, metadata

    def transform_dm(self, dm):
        """Transforming the delay matrix to fulfill back and forth latency equality

        Args:
            dm (numpy.ndarray): Delay matrix we will transform

        Returns:
            (numpy.ndarray): Returns with the transformed delay matrix
        """
        length = len(dm)
        for i in range(length):
            for j in range(length):
                if dm[i, j] != dm[j, i]:
                    val = max(dm[i, j], dm[j, i])
                    dm[i, j] = val
                    dm[j, i] = val
        return dm


def main():
    print("Start labeling...")
    labeler = LatencyMonitor()
    rtt_matrix = labeler.measurement()
    print("RTT MATRIX:")
    print(rtt_matrix)
    print("DONE")


if __name__ == '__main__':
    main()

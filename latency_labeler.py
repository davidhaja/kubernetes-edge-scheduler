#!/usr/bin/env python3

from os import path

from kubernetes import client, config, utils
from kubernetes.client.apis import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
import time
import itertools
import numpy as np

try:
    config.load_kube_config()
except FileNotFoundError as e:
    print("WARNING %s\n" % e)
    config.load_incluster_config()
api = core_v1_api.CoreV1Api()


def get_worker_node_names():
    return [node.metadata.name for node in (api.list_node(watch=False)).items if "master" not in node.metadata.name]


def create_pod_template(pod_name, node_name):
    # Configureate Pod template container
    container = client.V1Container(
        name=pod_name,
        image='docker.io/centos/tools:latest',
        command=['/sbin/init'])

    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(name=pod_name),
        spec=client.V1PodSpec(containers=[container], node_selector={"kubernetes.io/hostname": node_name}))

    return template


def deploy_rtt_deployment(pod_IPs, pod_node_mapping):
    for pod, pod_ip in pod_IPs.items():
        if pod_ip == None:
            template = create_pod_template(pod, pod_node_mapping[pod])

            api_instance = client.CoreV1Api()
            namespace = 'default'
            body = client.V1Pod(metadata=template.metadata, spec=template.spec)
            api_response = api_instance.create_namespaced_pod(namespace, body)


def check_rtt_deployment(ping_pods):
    for pod in ping_pods:
        running = False
        time_out = 120
        cur_time = 0
        while cur_time < time_out:
            resp = api.read_namespaced_pod(name=pod, namespace='default')
            if resp.status.phase == 'Running':
                running = True
                break
            time.sleep(1)
            cur_time += 1
        if not running:
            raise Exception("TIMEOUT: Pod {} is not running".format(pod))


def get_ping_pod_IPs(ping_pods, pod_IPs):
    ret = api.list_pod_for_all_namespaces(watch=False)
    for i in ret.items:
        if str(i.metadata.name) in ping_pods:
            pod_IPs[i.metadata.name] = i.status.pod_ip

    return pod_IPs


def measure_latency(pod_from, pod_to_IP):
    namespace = 'default'

    exec_command = ['/bin/sh', '-c', 'ping -c 5 {}'.format(pod_to_IP)]

    resp = stream(api.connect_get_namespaced_pod_exec, pod_from, namespace,
                  command=exec_command,
                  stderr=True, stdin=False,
                  stdout=True, tty=False)

    rtt_line = next(line for line in resp.split('\n') if 'rtt min/avg/max/mdev' in line)
    min_rtt = rtt_line.split('/')[3]
    avg_rtt = rtt_line.split('/')[4]
    max_rtt = rtt_line.split('/')[5]
    return float(avg_rtt)


def get_rtt_labels_of_node(node, rtt_matrix, ping_pods, POD_NODES_MAP):
    # FIXME: Currently we handle RTTs in millisec
    rtt_list = {}

    for i in ping_pods:
        if i != node:
            rtt = rtt_matrix[(node, i)]
            rtt_ms = int(round(rtt))
            try:
                rtt_list["rtt-{}".format(rtt_ms)].append(POD_NODES_MAP[i])
            except:
                rtt_list["rtt-{}".format(rtt_ms)] = [POD_NODES_MAP[i]]

    return rtt_list


def do_labeling(node_name, labels):
    # FIXME: This method could be stucked into a unvalid state: if we already delete the old rtt labels,
    #        but due to a failure, the new labels are not saved

    # Get old labels
    old_labels = []
    ret = api.list_node(watch=False)
    for node in ret.items:
        for label in node.metadata.labels:
            if "rtt" in label:
                if label not in old_labels:
                    old_labels.append(label)

    # Delete old labels
    old_labels_dict = {label: None for label in old_labels}
    api_instance = client.CoreV1Api()
    body = {
        "metadata": {
            "labels": old_labels_dict
        }
    }
    api_response = api_instance.patch_node(node_name, body)

    # Upload new labels
    for key, value in labels.items():
        api_instance = client.CoreV1Api()
        label_value = "_".join(value)
        body = {
            "metadata": {
                "labels": {
                    key: label_value
                }
            }
        }
        api_response = api_instance.patch_node(node_name, body)


def do_measuring(pod_IPs, ping_pods):
    ping_pods_permutations = list(itertools.permutations(ping_pods, 2))
    rtt_matrix = {(i, j): np.inf for (i, j) in ping_pods_permutations}
    for i, j in ping_pods_permutations:
        if rtt_matrix[(i, j)] == np.inf:
            print("\tMeasuring {} <-> {}".format(i, j))
            rtt_matrix[(i, j)] = measure_latency(i, pod_IPs[j])
            # FIXME: Now we assume that rtt i->j is the same than rtt j->i
            rtt_matrix[(j, i)] = rtt_matrix[(i, j)]
    return rtt_matrix


def labeling():
    nodes = get_worker_node_names()
    ping_pod_list = ["ping-pod{}".format(i) for i in range(1, len(nodes) + 1)]
    pod_nodes_mapping = {ping_pod_list[i]: nodes[i] for i in range(len(ping_pod_list))}
    pod_IPs = {ping_pod_list[i]: None for i in range(len(ping_pod_list))}
    pod_IPs = get_ping_pod_IPs(ping_pod_list, pod_IPs)

    # Deploy latency measurement pods
    deploy_rtt_deployment(pod_IPs, pod_nodes_mapping)
    check_rtt_deployment(ping_pod_list)

    pod_IPs = get_ping_pod_IPs(ping_pod_list, pod_IPs)

    # Measure latency
    rtt_matrix = do_measuring(pod_IPs, ping_pod_list)

    # Do labeling
    for pod in ping_pod_list:
        labels = get_rtt_labels_of_node(pod, rtt_matrix, ping_pod_list, pod_nodes_mapping)
        do_labeling(pod_nodes_mapping[pod], labels)

    return rtt_matrix


def main():
    print("Start labeling...")
    rtt_matrix = labeling()
    print("RTT MATRIX:")
    print(rtt_matrix)
    print("DONE")


if __name__ == '__main__':
    main()

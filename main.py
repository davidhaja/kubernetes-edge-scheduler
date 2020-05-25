#!/usr/bin/env python3
import kubernetes
from kubernetes.client.rest import ApiException
from latency_monitor import LatencyMonitor
from kubernetes import client, watch
from scheduler import CustomScheduler
import time
from threading import Thread
from configs import config as custom_config
import argparse
import json
import yaml
import os
import logger
import numpy as np


def main(simulation, metadata_path=None, rtt_matrix_path=None, nodes=None, pods=None):
    """Main function that starts the monitoring and scheduling

    Args:
        simulation (bool): Whether the underlying system is simulated
        metadata_path (str): Path of metadata file
        rtt_matrix_path (str): Path of delay matrix file
        nodes (str): Path of nodes file
        pods (str): Path of Pods file

    """
    print("Custom Scheduler is starting...")
    latency_labeler = LatencyMonitor()
    if not rtt_matrix_path:
        print("\tInit measuring...")
        if not custom_config.GOLDPINGER:
            rtt_matrix, metadata = latency_labeler.measurement()
            print("Measuring finished")
        else:
            rtt_matrix, metadata = latency_labeler.init_goldpinger()
    else:
        rtt_matrix = np.loadtxt(rtt_matrix_path)
        with open(metadata_path) as metadata_json:
            metadata = json.load(metadata_json)
    scheduler = CustomScheduler(simulation=simulation, delay_matrix=rtt_matrix, metadata=metadata, nodes=nodes)
    print("Custom Scheduler started")
    if not rtt_matrix_path:
        if not custom_config.GOLDPINGER:
            Thread(target=latency_labeler.measure_latency_with_ping_pods, args=(scheduler,)).start()
        else:
            Thread(target=latency_labeler.measure_latency_with_goldpinger, args=(scheduler,)).start()
    else:
        Thread(target=latency_labeler.watch_delay_file, args=(scheduler, rtt_matrix_path, metadata_path)).start()
    if not simulation:
        watch_real_k8s_events(scheduler)
    else:
        previous_print_string = ''
        placed_pods = 0
        for pod in pods:
            k8s_pod = parse_yaml_to_pod(pod)
            scheduler.schedule(k8s_pod)
            placed_pods += 1
            print(placed_pods)
            # print_info(scheduler, previous_print_string)
        print_info(scheduler, previous_print_string)


def get_request_yamls(pods_path):
    """Read the YAML files from the given path

    Args:
        pods_path (str): Path of directory with YAML files

    Returns:
        (list[str]): Returns the paths for the YAML files
    """
    if os.path.isdir(pods_path):
        if pods_path[-1] != '/':
            pods_path += '/'
        pod_files = [pods_path+f for f in os.listdir(pods_path) if os.path.isfile(pods_path+f)]
    else:
        pod_files = [pods_path]
    return pod_files


def parse_yaml_to_pod(pod_file):
    """Parse YAML request file and create V1Pod object from it

    Args:
        pod_file (str): Path of Pod file

    Returns:
        (kubernetes.client.V1Pod): Returns the constructed V1Pod object
    """
    with open(pod_file) as f:
        event = yaml.safe_load(f)
    metadata = kubernetes.client.V1ObjectMeta(**event['spec']['template']['metadata'])
    v1containers = []
    for v1container_dict in event['spec']['template']['spec']['containers']:
        v1container = kubernetes.client.V1Container(**v1container_dict)
        v1container.resources = kubernetes.client.V1ResourceRequirements(**v1container_dict['resources'])
        v1containers.append(v1container)
    spec = kubernetes.client.V1PodSpec(scheduler_name=event['spec']['template']['spec']['schedulerName'],
                                       containers=v1containers)
    k8s_pod = kubernetes.client.V1Pod(api_version=event['apiVersion'], kind=event['kind'], metadata=metadata,
                                      spec=spec)
    return k8s_pod


def watch_real_k8s_events(scheduler):
    """Watching the Kubernetes events
       We care about events with ADDED or DELETED type and events and events that has our scheduler name

    Args:
        scheduler (scheduler.CustomScheduler): The CustomScheduler we notify

    """
    w = watch.Watch()
    previous_print_string = ''
    # FIXME: API BUG: https://github.com/kubernetes-client/python/issues/547 -> we assume all scheduling will be OK
    for event in w.stream(scheduler.v1.list_namespaced_pod, "default"):
        if event['object'].status.phase == "Pending" and event['type'] == "ADDED" and \
           event['object'].spec.scheduler_name == scheduler.scheduler_name:
            try:
                print("Creating pod - named {} - request received".format(event['object'].metadata.name))
                res = scheduler.schedule(event['object'])
            except client.rest.ApiException as e:
                # print(json.loads(e.body)['message'])
                pass
        if event['type'] == "DELETED" and event['object'].spec.scheduler_name == scheduler.scheduler_name:
            scheduler.delete(event['object'])
        print_info(scheduler, previous_print_string)


def print_info(scheduler, previous_print_string):
    """Printing out the changes after processing the event
    
    Args:
        scheduler (scheduler.CustomScheduler): CustomScheduler that made the changes
        previous_print_string (str): Previously printed message

    """
    print_string_list = ["\n---------------------------------------------------"]
    for name, node in scheduler.nodes.items():
        if node.placeholder is not None or len(node.pods) > 0:
            if node.placeholder is not None:
                print_string_list.append("%s: Available memory: %d Pods: %s Placeholder pods: %s" %
                                     (node.name, node.available_memory, str(node.pods), str(node.placeholder.pods)))
            else:
                print_string_list.append("%s: Available memory: %d Pods: %s" %
                                         (node.name, node.available_memory, str(node.pods)))
    print_string = '\n'.join(print_string_list)
    if print_string != previous_print_string:
        print(print_string)
        previous_print_string = print_string
        # print("Event: %s %s %s %s %s" % (event['type'], event['object'].kind, event['object'].metadata.name,
        #                               event['object'].spec.scheduler_name, event['object'].status.phase))


def simulation(metadata_path=None, rtt_matrix_path=None, nodes=None, pods=None, both=False):
    """Runing the CustomScheduler with simulated environment

    Args:
        metadata_path (str): Path of metadata file
        rtt_matrix_path (str): Path of delay matrix file
        nodes (str): Path of nodes file
        pods (str): Path of Pod request file(s)
        both (bool): Whether do the simulation both with and without clustering

    """
    print("Loading delay matrix ...")
    rtt_matrix = np.loadtxt(rtt_matrix_path)
    print("Delay matrix loaded")

    print("Loading metadata ...")
    with open(metadata_path) as metadata_json:
        metadata = json.load(metadata_json)
    print("Metadata loaded")

    custom_config.RESULTS_DIR = 'simulations/temp/'
    dur_1 = do_one_scenario(True, pods, rtt_matrix, metadata, nodes)
    if both:
        custom_config.CLUSTERING = not custom_config.CLUSTERING
        dur_2 = do_one_scenario(True, pods, rtt_matrix, metadata, nodes)
        print('{}: {}'.format(custom_config.CLUSTERING, dur_2))
    print("Total with: {}".format(dur_1))


def do_one_scenario(simulation, pods, rtt_matrix, metadata, nodes):
    """Executing one scenario in the simulation

    Args:
        simulation (bool): Whether the underlying system is simulated
        pods (str): Path of Pod request file(s)
        rtt_matrix (numpy.ndarray): Delay matrix that contains the delay values between the nodes
        metadata (dict[str, int])): Metadata for delay matrix
        nodes (str): Path of nodes file

    Returns:
        (float): Total scheduling time
    """
    pod_files = get_request_yamls(pods)
    previous_print_string = ''
    placed_pods = 0
    print("Custom Scheduler is starting...")
    scheduler = CustomScheduler(simulation=simulation, delay_matrix=rtt_matrix, metadata=metadata, nodes=nodes)
    print("Custom Scheduler started")
    sched_total = 0.0
    import random
    random.shuffle(pod_files)
    for pod in pod_files:
        k8s_pod = parse_yaml_to_pod(pod)
        previous_element_name = next(x for x in k8s_pod.metadata.labels.keys() if 'delay_' in x).split('_')[1]
        if 'C' not in previous_element_name:
            sched_start = time.time()
            scheduler.schedule(k8s_pod)
            sched_end = time.time()
            sched_total += (sched_end - sched_start)
            placed_pods += 1
            # print(placed_pods)
    # print_info(scheduler, previous_print_string)
    optim_duration, offline_placeh_size, online_placeh_size, pods_num = scheduler.optimize()
    print(custom_config.CLUSTERING)
    print(optim_duration)
    print('Offline size: {}'.format(offline_placeh_size))
    print('Online size: {}'.format(online_placeh_size))
    print(pods_num)
    if custom_config.CLUSTERING:
        print('Clustering online placeholders num: {}'.format(len(scheduler.placeholders)))
        print('Offline ratio: {}'.format(offline_placeh_size / (pods_num*scheduler.convert_to_int('1Gi')) * 100))
        print('Online ratio: {}'.format(online_placeh_size / (pods_num * scheduler.convert_to_int('1Gi')) * 100))
    else:
        print('NO Clustering online placeholders num: {}'.format(len(scheduler.placeholders)))
        print('Offline ratio: {}'.format(offline_placeh_size / (pods_num * scheduler.convert_to_int('1Gi')) * 100))
        print('Online ratio: {}'.format(online_placeh_size / (pods_num * scheduler.convert_to_int('1Gi')) * 100))
    return sched_total


def set_global_variables(node_num, mem, group_num):
    """Setting the global variables in logger.py

    Args:
        node_num (int): Number of nodes
        mem (str): Pods' memory request in string
        group_num (int): Grouping number in topology

    """
    logger.node_number = node_num
    logger.memory = mem
    logger.clustering_time = 0
    logger.group_num = group_num
    logger.write_result('', '',  operation='w')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Custom K8s scheduler')
    parser.add_argument('--delay_matrix', '-d', dest='delay_matrix', action="store",
                        help='Path to Delay Matrix file (measured delays between edge clusters)')
    parser.add_argument('--simulation', '-s', dest='simulation', action="store_true", default=False,
                        help='Simulate underlying Kubernetes environment')
    parser.add_argument('--nodes', '-n', dest='nodes', action="store",
                        help='Path to simulated nodes file')
    parser.add_argument('--metadata', '-m', dest='metadata', action="store",
                        help='Path to metadata file')
    parser.add_argument('--pods', '-p', dest='pods', action="store",
                        help='Path to simulated pod requests file')
    parser.add_argument('--both', '-b', dest='both', action="store_true", default=False,
                        help='Simulate both with and without clustering')
    args = parser.parse_args()
    if not args.simulation:
        main(args.simulation, args.metadata, args.delay_matrix, args.nodes, args.pods)
    else:
        simulation(args.metadata, args.delay_matrix, args.nodes, args.pods, args.both)

#!/usr/bin/env python3

import json
from kubernetes.client.rest import ApiException
import latency_labeler
from kubernetes import client, watch
from scheduler import CustomScheduler


def main():
    print("Custom Scheduler is starting...")
    print("\tInit measuring...")
    # TODO: We should do the measurement and the labeling periodically
    rtt_matrix = latency_labeler.labeling()
    print("Labeling finished")
    scheduler = CustomScheduler()
    w = watch.Watch()
    # FIXME: API BUG: https://github.com/kubernetes-client/python/issues/547 -> we assume all scheduling will be OK
    for event in w.stream(scheduler.v1.list_namespaced_pod, "default"):
        if event['object'].status.phase == "Pending" and event['type'] == "ADDED" and \
           event['object'].spec.scheduler_name == scheduler.scheduler_name:
            try:
                print("Creating pod - named {} - request received".format(event['object'].metadata.name))
                res = scheduler.schedule(event['object'])
            except client.rest.ApiException as e:
                print(json.loads(e.body)['message'])


if __name__ == '__main__':
    main()

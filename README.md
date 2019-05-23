# kubernetes-edge-scheduler
Kubernetes has become the most popular cluster manager during the past 5 years. It is used primarily for orchestrating data center deployments running web applications. Its powerful features, e.g., self-healing and scaling, have attracted a huge community, which in turn, is inducing a meteoric rise of this open source project. We venture to shape Kubernetes to be suited for edge infrastructure. As mostly delay-sensitive applications are to be deployed in the edge, a topology-aware Kubernetes is needed, extending its widely-used feature set with regard to network latency. Moreover, as the edge infrastructure is highly prone to failures and is considered to be expensive to build and maintain, self-healing features must receive more emphasis than in the baseline Kubernetes. We therefore designed a custom Kubernetes scheduler that makes its decisions with applications' delay constraints and edge reliability in mind. In this demonstration we show the novel features of our Kubernetes extension, and describe the solution that we release as open source.

Video showcase is available at https://www.youtube.com/watch?v=FL2OpHV6ZEs
More information at http://bit.ly/2QfVqxq

## Installation

### Requirements
In order to use our scheduler, you need the following components:
* Up and running Kubernetes cluster
* Python3
* Pip3

To install the required python packages use [requirements.txt](requirements.txt) with pip3 like:
`sudo pip3 install -r requirements.txt`

## How to use?
You can start custom-scheduler in developer mode and live testing easily with `python3 main.py` command.

### Start custom-scheduler as a pod at master node
We followed the standard Kubernetes tutorial for configuring multiple schedulers: https://kubernetes.io/docs/tasks/administer-cluster/configure-multiple-schedulers/
You can build your own Docker image using the given [Dockerfile](Dockerfile), or you can use our proposed prebuilt docker image.
If you use your own image, you have to change the image's value in the [configs/custom-scheduler.yaml](configs/custom-scheduler.yaml) before you deploy the custom-scheduler.
If you wish to use our proposed image, it is worth to pull the image from Docker Hub with `sudo docker pull davidhaja/kube-repo:demo2` command.
To deploy the custom-scheduler in the master node you can use [start-custom-scheduler.sh](demo/start-custom-scheduler.sh) script. This script creates a new role for the custom-scheduler, and deploys the scheduler instance binded with this new role.
After the deployment you can check if the custom-scheduler is running with `kubectl get pods --all-namespaces` command.

### Request examples
To define delay requirement for a pod, you have to create a new label in your request, where the label must look like `delay_<from>: "<delay value in ms>"`.
There are some example deployment in [demo](demo/) folder.
Also important key: value field in a request is `schedulerName: <scheduler name>`, which defines which kubernetes scheduler will handle the request. 
In our case we register our scheduler as: custom-scheduler .
An example deployment with specified delay requirement:
```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: deployment1
spec:
  replicas: 1
  selector:
    matchLabels:
      app: app1
  template:
    metadata:
      name: pod1
      labels:
        app: app1
        delay_edge-1: "25"
    spec:
      containers:
      - name: pod1-container
        image: docker.io/centos/tools:latest
        resources:
          limits:
            memory: "6Gi"
          requests:
            memory: "6Gi"
        command:
        - /sbin/init
      schedulerName: custom-scheduler

```
### Cloud role
Our proposed scheduler will look after nodes representing the worker machines from a cloud provider. 
To label a node as a worker from a cloud provider use `kubectl label node <node> kubernetes.io/role=cloud` command.


## Contacts
David Haja - haja@tmit.bme.hu, 
Mark Szalay - mark.szalay@tmit.bme.hu
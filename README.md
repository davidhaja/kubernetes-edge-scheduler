# kubernetes-edge-scheduler
Kubernetes has become the most popular cluster manager during the past 5 years. It is used primarily for orchestrating data center deployments running web applications. Its powerful features, e.g., self-healing and scaling, have attracted a huge community, which in turn, is inducing a meteoric rise of this open source project. We venture to shape Kubernetes to be suited for edge infrastructure. As mostly delay-sensitive applications are to be deployed in the edge, a topology-aware Kubernetes is needed, extending its widely-used feature set with regard to network latency. Moreover, as the edge infrastructure is highly prone to failures and is considered to be expensive to build and maintain, self-healing features must receive more emphasis than in the baseline Kubernetes. We therefore designed a custom Kubernetes scheduler that makes its decisions with applications' delay constraints and edge reliability in mind. In this demonstration we show the novel features of our Kubernetes extension, and describe the solution that we release as open source.

Video showcase is available at https://www.youtube.com/watch?v=9joijDvx-tM
More information at http://bit.ly/2QfVqxq

## Installation

### Requirements
In order to use our scheduler, you need the following components:
* Up and running Kubernetes cluster (Optional)
* Python3
* Pip3
* [Goldpinger](https://github.com/bloomberg/goldpinger) (Optional)

To install the required python packages use [requirements.txt](requirements.txt) with pip3 like:
`sudo pip3 install -r requirements.txt`

## How to use?
You can start custom-scheduler in developer mode, for testing, easily with `python3 main.py` command.
`python3 main.py --help` gives the available flags that can be used with main.py.

### Start custom-scheduler as a Pod at master node
We followed the standard Kubernetes tutorial for configuring multiple schedulers: https://kubernetes.io/docs/tasks/administer-cluster/configure-multiple-schedulers/
You can build your own Docker image using the given [Dockerfile](Dockerfile).
If you use your own image, you have to change the image value in the [configs/custom-scheduler.yaml](configs/custom-scheduler.yaml) before you deploy the custom-scheduler.
To deploy the custom-scheduler in the master node you can use [start-custom-scheduler.sh](demo/start-custom-scheduler.sh) script. This script creates a new role for the custom-scheduler, and deploys the scheduler instance binded with this new role.
After the deployment you can check if the custom-scheduler is running with `kubectl get pods --all-namespaces` command.

### Request examples
To define delay requirement for a Pod, you have to create a new label in your request, where the label must look like `delay_<from>: "<delay value in ms>"`.
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

### Network measurement
Measurements can be performed in three ways with our implementation: 
i) with our ping pods; 
ii) with Goldpinger (enable Goldpinger in [config.py](configs/config.py)); 
iii) with static files (execute [main.py](main.py) with the appropriate flags).
In the first scenario, our solution is able to start a lightweight Pod on each node, which Pods using Ping application 
provide the RTT (Round-trip time) between all worker nodes to our custom-scheduler. Goldpinger is a third party tool 
that runs as a DaemonSet on Kubernetes and produces metrics that can be scraped, visualised and alerted on. 
The third option is when the network metrics are provided in static files, which are processed by our solution.

### Clustering
Topology clustering based on service's delay requirement can be enabled in [config.py](configs/config.py). 
Some initial layer delays can also be defined in the config that will be used for clustering the topology before 
the services are requested. 
 
## Contacts
David Haja - haja@tmit.bme.hu, 
Mark Szalay - mark.szalay@tmit.bme.hu



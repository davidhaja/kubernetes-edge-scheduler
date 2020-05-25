
class Pod(object):
    """The Pod class used for representing the actual Pods inside the Kubernetes system
        The purpose of this class is, we don't want to query the Kubernetes system so many times.

    """

    def __init__(self, name, node, k8s_pod, memory_request, constraint=None, nodes_in_radius=None, cloudable=False,
                 pod_constraint=False):
        """Constructor of Pod

        Args:
            name (str): Name of the Pod
            node (node): The hosting node's name
            k8s_pod (kubernetes.client.V1Pod): The Kubernetes representation of the Pod
            memory_request (int): Required memory
            constraint (dict[str, int]): The delay constraint which has a reference element and a delay value
            nodes_in_radius (dict[str, baseclasses.node.Node]): Nodes that fit in the radius defined by the constraint
            cloudable (bool): If Pod can be deployed in the cloud
            pod_constraint (bool): If the previous (reference) element is a Pod
        """
        self.name = name
        if constraint is None:
            self.constraint = {}
        else:
            self.constraint = constraint
        if nodes_in_radius is None:
            self.nodes_in_radius = {}
        else:
            self.nodes_in_radius = nodes_in_radius
        self.node = node
        self.k8s_pod = k8s_pod
        self.cloudable = cloudable
        self.pod_constraint = pod_constraint
        self.memory_request = memory_request
        self.placeholder_id = None

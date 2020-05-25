
class Node(object):
    """Node class that represents a Kubernetes node in the custom-scheduler
       The purpose of this class is, we don't want to query the Kubernetes system so many times.

    """

    def __init__(self, name, k8s_node, memory, cluster=None, cloud=False, alive=True):
        """Constructor of Node

        Args:
            name (str): Name of the node
            k8s_node (kubernetes.client.V1Node): Kubernetes representation on node
            memory (int): The memory capacity of node
            cluster (): Currently unused
            cloud (bool): If the node represents the cloud
            alive (bool): If the node is alive (Ready)
        """
        self.name = name
        self.__max_memory = memory
        self.available_memory = memory
        self.pods = []
        self.placeholder = None
        self.cloud = cloud
        self.k8s_node = k8s_node
        self.cluster = cluster
        self.alive = alive
        self.placeable_pods = set()

    def get_max_memory(self):
        """Get the memory capacity

        Returns:
            (int): Returns the maximum memory
        """
        return self.__max_memory

    def add_mypod(self, mypod):
        """Add pod to this node

        Args:
            mypod (baseclasses.pod.Pod): The Pod we want do deploy

        """
        if self.available_memory < mypod.memory_request:
            raise Exception('Memory less than the pod request')
        self.available_memory -= mypod.memory_request
        if mypod.name in self.pods:
            raise Exception('Pod already in node')
        self.pods.append(mypod.name)

    def delete_mypod(self, mypod):
        """Delete Pod from this node

        Args:
            mypod (baseclasses.pod.Pod): The Pod we want to remove

        """
        self.available_memory += mypod.memory_request
        if self.available_memory > self.__max_memory:
            raise Exception('Memory greater than the max')
        if mypod.name not in self.pods:
            raise Exception('Pod not in node')
        self.pods.remove(mypod.name)

    def add_placeholder(self, placeholder):
        """Add placeholder to this node

        Args:
            placeholder (baseclassses.placeholder.Placeholder): Placeholder object we want to add

        """
        if self.available_memory < placeholder.required_memory:
            raise Exception('Memory less than the placeholder request')
        self.available_memory -= placeholder.required_memory
        if self.placeholder is not None:
            raise Exception('Placeholder already defined')
        self.placeholder = placeholder
        placeholder.node = self.name

    def delete_placeholder(self, placeholder):
        """Delete placeholder from this node

        Args:
            placeholder (baseclasses.placeholder.Placeholder): Placeholder object we want to remove

        """
        self.available_memory += placeholder.required_memory
        if self.available_memory > self.__max_memory:
            raise Exception('Memory greater than the max')
        if self.placeholder is None:
            raise Exception('Placeholder already deleted')
        self.placeholder = None

    def decrease_available_memory(self, size):
        """Decreasing the free memory on this node

        Args:
            size (int): The amount by which we reduce memory

        """
        if self.available_memory - size < 0:
            raise Exception("Negative memory on node: " + self.name)
        self.available_memory -= size


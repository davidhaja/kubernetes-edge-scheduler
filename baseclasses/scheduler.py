import numpy as np


class BaseScheduler(object):
    """Base class of the resource schedulers and orchestrators

    """

    def __init__(self, delay_matrix, metadata, nodes=None, pods=None):
        """

        Args:
            delay_matrix (numpy.matrix): The matrix that contains the delay values between the nodes
            metadata (dict[str, int]): Since numpy.matrix cannot contain labels, we store the indexes of the nodes
            nodes (dict[str, baseclasses.node.Node]): The already parsed nodes
            pods (dict[str, baseclasses.pod.Pod]): The set of Pods
        """
        self.placeholders = dict()
        self.delay_matrix = delay_matrix
        self.nodes = nodes
        self.pods = dict() if not pods else pods
        self._dm_metadata = metadata
        self._reverse_metadata = {value: key for key, value in self._dm_metadata.items()}

    def get_placeholder_by_pod(self, pod):
        """Get the palceholder that covers the given Pod

        Args:
            pod (Optional[str, baseclasses.pod.Pod]): The examined Pod

        Returns:
            (baseclasses.placeholder.Placeholder): Placeholder object that covers the given Pod
        """
        try:
            if isinstance(pod, str):
                return next(self.placeholders[x] for x in self.placeholders if pod in self.placeholders[x].pods)
            else:
                return next(self.placeholders[x] for x in self.placeholders if pod.name in self.placeholders[x].pods)
        except StopIteration:
            return None

    def get_nodes_in_radius(self, previous_element_name, required_delay):
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

    def get_node_from_podname_or_nodename(self, previous_element_name):
        """Get the node that holds the given Pod's name, or has the name defined in the parameter

        Args:
            previous_element_name (str): Name of the Pod or node

        Returns:
            (baseclasses.node.Node): Node object that is defined by the parameter
        """
        node = self.get_node_from_name(previous_element_name)
        if node is None:
            return self.get_node_from_pod(previous_element_name)
        else:
            return node

    def get_node_from_name(self, node_name):
        """Get the node with the given name

        Args:
            node_name (str): Name of the node

        Returns:
            (baseclasses.node.Node): Returns the node object with the given name
        """
        try:
            return self.nodes[node_name]
        except KeyError:
            return None

    def get_node_from_pod(self, previous_element_name):
        """Get the node that hosts the given Pod

        Args:
            previous_element_name (str): The name of the Pod

        Returns:
            (baseclasses.node.Node): Returns the node that hosts the Pod with the given name
        """
        pod = self.get_pod_by_name(previous_element_name)
        node = self.get_node_from_name(pod.node)
        return node

    def get_pod_by_name(self, name):
        """Get the Pod object by its name

        Args:
            name (str): Name of the Pod

        Returns:
            (baseclasses.pod.Pod): Pod object with the given name
        """
        try:
            return self.pods[name]
        except KeyError:
            return next(self.pods[x] for x in self.pods if name in x)

    def choose_min_utilized_by_mem(self, nodes):
        """Choose the node with the most available memory

        Args:
            nodes (dict[str, baseclasses.node.Node]): The nodes we choose from

        Returns:
            (baseclasses.node.Node): The chosen node
        """
        return self.get_node_from_name(sorted(nodes, key=lambda x: (-1 * nodes[x].available_memory, nodes[x].name))[0])

    def choose_min_utilized_by_num_of_pods(self, nodes):
        """Choose the node that has the least Pods and has the least memory utilization

        Args:
            nodes (dict[str, baseclasses.node.Node]): The nodes we choose from

        Returns:
            (baseclasses.node.Node): The chosen node
        """
        return self.get_node_from_name(sorted(nodes,
                                              key=lambda x:
                                              (len(nodes[x].pods),
                                               -1 * nodes[x].available_memory/nodes[x].get_max_memory()))[0])

    def choose_min_utilized_by_mem_ratio(self, nodes):
        """Choose the node that has the least memory utilization

        Args:
            nodes (dict[str, baseclasses.node.Node]): The nodes we choose from

        Returns:
            (baseclasses.node.Node): The chosen node
        """
        return self.get_node_from_name(sorted(nodes,
                                              key=lambda x:
                                              (-1 * nodes[x].available_memory/nodes[x].get_max_memory()))[0])

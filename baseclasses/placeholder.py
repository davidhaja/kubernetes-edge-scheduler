
class Placeholder(object):
    """Placeholders are provisioned resources that provides high-reliability for Pods
        with the awareness of their delay requirement

    """

    def __init__(self, node, required_memory=0):
        """Constructor of Placeholder

        Args:
            node (str): Name of the node, where the placeholder is provisioned
            required_memory (int): The amount of memory that the placeholder requires
        """
        self.required_memory = required_memory
        self.pods = set()
        self._node = None
        self._name = None

        self._node = node
        self.id = node

    @property
    def node(self):
        """The name of the node that hosts the placeholder
            The placeholder's id is equal with the node name

        Returns:
            (str): node
        """
        return self._node

    @node.setter
    def node(self, node):
        self._node = node
        self.id = node

    @property
    def name(self):
        """The name of the placeholder

        Returns:
            (str):
        """
        return self._name

    @name.setter
    def name(self, name):
        self._name = name
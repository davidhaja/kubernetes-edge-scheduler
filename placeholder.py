

class Placeholder(object):

    def __init__(self, required_memory=0, node=None, name=None):
        self.required_memory = required_memory
        self.pods = set()
        self._node = None
        self._name = None

        if node is not None:
            self._node = node
        if name is not None:
            self._name = name

    @property
    def node(self):
        """

        :rtype: str
        """
        return self._node

    @node.setter
    def node(self, node):
        """

        :param node:
        :type: str
        """
        self._node = node

    @property
    def name(self):
        """

        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name):
        """

        :param name:
        :type: str
        """
        self._name = name
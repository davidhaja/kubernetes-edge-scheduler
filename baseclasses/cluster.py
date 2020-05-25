import uuid
import networkx as nx
import matplotlib.pyplot as plt
import copy
import time
from logger import write_result
import numpy as np


class Cluster(object):
    """Cluster class that built by the clusterer

    """

    def __init__(self, name, nodes, parent=None):
        """Constructor of Cluster object

        Args:
            name (str): Name of the cluster
            nodes (set[str]): Set of node names inside the cluster
            parent (): Currently unused
        """
        self.name = str(uuid.uuid4()) if not name else name
        self.nodes = nodes
        self.parent = parent


class BaseClusterer(object):
    """Base clusterer class

    """

    def __init__(self, delay_matrix, graph=None, service_delays=None):
        """Constructor of BaseClusterer

        Args:
            delay_matrix (numpy.ndarray): Matrix that holds the delay values between each node
            graph (nx.Graph): Graph that presents the connection of nodes
            service_delays (set[int]): Set of delays that define the cluster layers
        """
        self.delay_matrix = delay_matrix
        self.graph = graph
        self.service_delays = set() if not service_delays else service_delays

    def run_algo(self, graph, algo, name):
        """Run a clustering algorithm and measure its execution time

        Args:
            graph (nx.Graph): Graph that present the connection of nodes
            algo (function): The algorithm that will be executed
            name (str): The name of the algorithm

        Returns:
            (Any): Return the result created by the given algorithm
        """
        start = time.time()
        print("{} clustering algorithm start: {}".format(name, start))
        # This cluster call is where most of the heavy lifting happens
        result = algo(graph)
        end = time.time()
        print("{} clustering algorithm end: {}".format(name, end))
        print("{} clustering algorithm duration: {:.4f} sec".format(name, end - start))

        return result


class DynamicClusterer(BaseClusterer):
    """DynamicClusterer class
        Based on Pods/services delay requirement, it creates cluster layers from the topology.

    """

    def __init__(self, delay_matrix, metadata, graph=None, service_delays=None):
        """Constructor of DynamicClusterer

        Args:
            delay_matrix (numpy.ndarray): The matrix that holds the delay values between each nodes
            metadata (dict[str, int]): Since numpy.ndarray cannot contain labels, we store the indexes of the nodes
            graph (networkx.Graph): The graph the presents the connection of the nodes
            service_delays (set): Set of service delay requirements
        """
        super(DynamicClusterer, self).__init__(delay_matrix, graph, service_delays)
        self.delay_matrix = copy.copy(delay_matrix)
        self.metadata = metadata
        self.reverse_metadata = dict([(value, key) for key, value in self.metadata.items()])
        self.clustering_layers = {}
        self.create_initial_layer()
        vantage_start = time.time()
        self.find_vantage_free_delays()
        vantage_end = time.time()
        print(vantage_end-vantage_start)
        print('vantage_free_delays: {}'.format(list(self.clustering_layers.keys())))
        write_result('vantage duration', vantage_end-vantage_start)
        write_result('vantage_free_delays', self.clustering_layers.keys())

    def find_vantage_free_delays(self):
        """Finding vantage point free delay values, and the appropriate clusters

        """
        values = sorted(set(self.delay_matrix.flatten()), reverse=True)
        delay_matrix_copy = copy.copy(self.delay_matrix)
        delay_matrix_copy[delay_matrix_copy > values[0]] = 0
        graph = nx.from_numpy_matrix(self.delay_matrix)
        graph = nx.relabel_nodes(graph, self.reverse_metadata)
        for i in range(len(values)):
            delay = values[i]
            new_clusters = {}
            if i != 0:
                graph.remove_edges_from([(x[0], x[1]) for x in graph.edges.data() if x[2]['weight'] > delay])
            if delay != 0:
                vantage_free = True
                node_sets = list(nx.connected_components(graph))
                if len(node_sets) != len(self.delay_matrix):
                    for n in node_sets:
                        if len(n) > 1:
                            complete_degree = len(n)-1
                            if any(graph.degree(x) != complete_degree for x in n):
                                # This component is not a complete graph,
                                # therefore no vantage point free clustering available for this delay
                                vantage_free = False
                                break
                if vantage_free:
                    for node_set in node_sets:
                        cluster_name = '_'.join(node_set)
                        for n in node_set:
                            self.clustering_layers[0]['nodes'][n][delay] = node_set
                        cluster = Cluster(name=cluster_name, nodes=node_set)
                        new_clusters[cluster_name] = cluster
                    if len(new_clusters) == 1:
                        self.clustering_layers[delay] = {'clusters': new_clusters, 'vantage_free': True}
                    else:
                        new_layer_delay_matrix, new_metadata = self.aggregate(new_clusters, self.clustering_layers[0])
                        self.clustering_layers[delay] = {'clusters': new_clusters,
                                                         'delay_matrix': new_layer_delay_matrix,
                                                         'metadata': new_metadata, 'vantage_free': True}
                    self.service_delays.add(delay)

    def get_nodes_in_radius(self, previous_node_name, delay):
        """Get the nodes/clusters that are in the radius of the previous element with the given delay value

        Args:
            previous_node_name (str): The name of the previous element.
            delay (int): The delay value that gives the maximum distance from the previous element

        Returns:
            (list[str]): The name of the nodes inside the found cluster
        """
        if delay in self.clustering_layers:
            try:
                cluster = self.clustering_layers[0]['nodes'][previous_node_name][delay]
            except KeyError:
                return self.clustering_layers[0]['nodes'].keys()
        else:
            layer_delay, underlying_layer = self.get_underlying_layer(delay)
            if len(underlying_layer['clusters']) == 1:
                cluster = underlying_layer['clusters'][next(iter(underlying_layer['clusters']))]
                self.clustering_layers[delay] = {'clusters': {cluster.name: cluster}}
                return cluster.name.split('_')
            else:
                new_clusters = self.new_service(delay)
                cluster = self.clustering_layers[0]['nodes'][previous_node_name][delay]
        return cluster

    def get_cluster_from_node(self, previous_node_name, underlying_layer):
        """Get the cluster that contains the given node in the given layer

        Args:
            previous_node_name (str): The node's name
            underlying_layer (dict): The underlying layer, that contains the clusters

        Returns:
            (Cluster): The cluster that has the given node defined by its name
        """
        # FIXME: We should use a more sophisticated way for finding a cluster from a worker node name
        return next(underlying_layer['clusters'][x] for x in underlying_layer['clusters'] if previous_node_name in x)

    def create_initial_layer(self):
        """Creating initial cluster layer.
            Since this is a agglomerative clustering algorithm, we separate each node in its own cluster.

        """
        initial_layer = {}
        nodes = {}
        for node in self.metadata.keys():
            initial_layer[node] = Cluster(name=str(node), nodes={node})
            nodes[node] = {}
        self.clustering_layers[0] = {'clusters': initial_layer, 'delay_matrix': self.delay_matrix,
                                     'metadata': self.metadata, 'nodes': nodes}
        self.service_delays.add(0)

    def new_services(self, service_delays):
        """New delay requirements that are used for clustering

        Args:
            service_delays (list[int]): List of service delays

        """
        for s_d in service_delays:
            self.new_service(s_d)

    def new_service(self, service_delay):
        """New delay requirement that is used for creating cluster layer

        Args:
            service_delay (int): Delay requirement

        Returns:
            (dict[str, Cluster]): Returns the new clusters that are constructed using the given delay
        """
        if service_delay not in self.service_delays:
            clustering_start = time.time()
            print('New clustering layer with {} ms delay'.format(str(service_delay)))
            layer_delay, previous_layer = self.get_underlying_layer(service_delay)
            if len(previous_layer['clusters']) == 1:
                cluster = previous_layer['clusters'][next(iter(previous_layer['clusters']))]
                self.clustering_layers[service_delay] = {'clusters': {cluster.name: cluster}}
                return self.clustering_layers[service_delay]['clusters']
            new_clusters = self.run(previous_layer=previous_layer, service_delay=service_delay)
            new_layer_delay_matrix, new_metadata = self.aggregate(new_clusters, previous_layer)
            for n in new_clusters:
                splitted = n.split('_')
                for nn in splitted:
                    self.clustering_layers[0]['nodes'][nn][service_delay] = set(splitted)

            self.clustering_layers[service_delay] = {'clusters': new_clusters,
                                                     'delay_matrix': new_layer_delay_matrix,
                                                     'metadata': new_metadata}
            self.service_delays.add(service_delay)
            print('{} cluster(s) in the new layer'.format(str(len(new_clusters))))
            clustering_end = time.time()
            write_result('{}ms clustering'.format(str(service_delay)), clustering_end-clustering_start)
            return new_clusters
        else:
            return self.clustering_layers[service_delay]['clusters']

    def aggregate(self, new_clusters, previous_layer):
        """Aggregation of fresh cluster layer.
            In this method, we create the new delay matrix that belongs to the new clusters
            and constructed from the underlying layer

        Args:
            new_clusters (dict): Dictionary of new clusters
            previous_layer (dict): The underlying layer

        Returns:
            (numpy.array, dict[str, int]): Returns with the new delay matrix and metadata that belongs to new clusters
        """
        previous_dm = previous_layer['delay_matrix']
        previous_metadata = previous_layer['metadata']
        new_dm = []
        new_metadata = {}
        new_index = 0
        for i in new_clusters:
            row = []
            new_metadata[i] = new_index
            new_index += 1
            for j in new_clusters:
                if i not in previous_layer['clusters'] and j not in previous_layer['clusters']:
                    if i == j:
                        row.append(0)
                    else:
                        row.append(self.get_max_delay_between_clusters(new_clusters[i].nodes, new_clusters[j].nodes,
                                                                       previous_dm, previous_metadata))
                elif i not in previous_layer['clusters']:
                    row.append(self.get_max_delay_between_clusters(new_clusters[i].nodes, [j], previous_dm,
                                                                   previous_metadata))
                elif j not in previous_layer['clusters']:
                    row.append(self.get_max_delay_between_clusters([i], new_clusters[j].nodes, previous_dm,
                                                                   previous_metadata))
                else:
                    # i and j in previous layer
                    row.append(previous_dm[previous_metadata[i]][previous_metadata[j]])
            new_dm.append(row)
        df = np.array(new_dm)
        return df, new_metadata

    def get_max_delay_between_clusters(self, c1, c2, delay_matrix, metadata):
        """Get the maximum delay between each cluster.

        Args:
            c1 (list[str]): List of node names in the cluster 1
            c2 (list[str]): List of node names in the cluster 2
            delay_matrix (numpy.ndarray): The actual delay matrix
            metadata (dict[str, int]): Metadata for showing which numpy row and column which node

        Returns:
            (int): Returns the maximum of all delays between each node in the clusters
        """
        max_d = delay_matrix[[metadata[x] for x in c1], :][:, [metadata[y] for y in c2]].max()
        return max_d

    def get_underlying_layer(self, service_delay):
        """Get the underlying layer for the given service delay
           It finds the underlying layer that is clustered with the delay that is the less and closest to the new one.

        Args:
            service_delay (int): The delay value

        Returns:
            (int, dict): Returns with the delay that belongs to the found underlying layer and the layer itself
        """
        layer_delay = max([k for k in self.clustering_layers if k <= service_delay])
        return layer_delay, self.clustering_layers[layer_delay]

    def get_underlying_vantage_free_layer(self, service_delay):
        """Get the underlying vantage point free layer for the given service delay
           It finds the underlying layer that is clustered with the delay that is the less and closest to the new one
           and vantage point free.

        Args:
            service_delay (int): The delay value

        Returns:
            (int, dict): Returns with the delay that belongs to the found underlying layer and the layer itself
        """
        layer_delay = max([k for k in self.clustering_layers if k <= service_delay and
                           'vantage_free' in self.clustering_layers[k] and self.clustering_layers[k]['vantage_free']])
        return layer_delay, self.clustering_layers[layer_delay]

    def run(self, previous_layer, service_delay):
        """Copy and prepare the delay matrix for creating new clusters, and do the clustering

        Args:
            previous_layer (dict): The underlying clustering layer
            service_delay (int): The new delay request

        Returns:
            (dict[str, Cluster]): Returns the dictionary that has the new clusters with cluster name: Cluster key-values
        """
        previous_layer_clusters = previous_layer['clusters']
        previous_delay_matrix = previous_layer['delay_matrix']
        previous_metadata = previous_layer['metadata']

        delay_matrix_copy = copy.copy(previous_delay_matrix)
        delay_matrix_copy[delay_matrix_copy > service_delay] = 0

        new_clusters = self.do_clustering(previous_layer_clusters, delay_matrix_copy, previous_metadata)
        return new_clusters

    def do_clustering(self, previous_layer_clusters, delay_matrix, previous_metadata):
        """Core of creating new clusters
           We create a graph from the modified delay matrix, find the cliques in it, and do the clustering based on the
           cliques' size

        Args:
            previous_layer_clusters (dict): The underlying layer
            delay_matrix (numpy.ndarray): The delay matrix
            previous_metadata (dict[str, int]): The underlying layer's metadata

        Returns:
            (dict[str, Cluster]): Returns the dictionary that has the new clusters with cluster name: Cluster key-values
        """
        new_clusters = {}
        graph = nx.from_numpy_matrix(delay_matrix)
        graph = nx.relabel_nodes(graph, dict([(value, key) for key, value in previous_metadata.items()]))
        cliques = sorted(nx.find_cliques(graph), key=lambda x: len(x), reverse=True)
        chosen_index = 0
        while any(len(x) > 0 for x in cliques):
            max_len = 0
            clique = cliques.pop(chosen_index)
            chosen_index = 0
            if len(clique) == 1:
                new_clusters[clique[0]] = previous_layer_clusters[clique[0]]
            else:
                cluster_name = '_'.join(clique)
                cluster = Cluster(name=cluster_name, nodes=set(clique))
                new_clusters[cluster_name] = cluster
            cliques = list(filter(None, cliques))
            for i in range(len(cliques)):
                cliques[i] = list(set(cliques[i]).difference(set(clique)))
                if len(cliques[i]) > max_len:
                    chosen_index = i
                    max_len = len(cliques[i])
        # TODO: set parents?
        return new_clusters

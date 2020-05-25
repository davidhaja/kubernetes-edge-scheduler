from configs import config
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

node_number = 0
memory = 0
clustering_time = 0
group_num = 0
iteration = 0


def write_result(result_msg, result, operation='a'):
    """Writing message and result value to file

    Args:
        result_msg (str): Message
        result (Any): The result
        operation (str): The operation of file access

    """
    result_path = config.RESULTS_DIR
    try:
        result_path +='{}-{}/'.format(group_num, memory)
        os.mkdir(result_path)
    except FileExistsError:
        pass
    result_file = result_path+'{}node-{}-{}-{}-{}-result.txt'.format(node_number, group_num, memory, config.CLUSTERING,
                                                                     iteration)
    try:
        with open(result_file, operation) as f:
            f.write("{}: {} \n".format(result_msg, str(result)))
            if 'clustering' in result_msg:
                global clustering_time
                clustering_time = clustering_time + result
            if 'Total' in result_msg and config.CLUSTERING:
                f.write("{}: {} \n".format('Total clustering time', str(clustering_time)))
                f.write("{}: {} \n".format('Total schedule time', str(result-clustering_time)))
    except Exception as e:
        print(e)


def convert_to_int(resource_string):
    if 'Ki' in resource_string:
        return int(resource_string.split('K')[0])*1024
    elif 'Mi' in resource_string:
        return int(resource_string.split('M')[0])*(1024**2)
    elif 'Gi' in resource_string:
        return int(resource_string.split('G')[0])*(1024**3)


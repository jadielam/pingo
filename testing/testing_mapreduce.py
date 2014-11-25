'''
Created on Nov 24, 2014

@author: jadiel
'''
from mapreduce.mapreduce import MapReduceManager
import multiprocessing

#The map and reduce function of this example will produce as output a count of each number on the list

def map_function(input_values):
    
    '''
    map_function takes as input a list of values, and returns as output a list of key-value pairs
    '''
    
    to_return = list()
    for i in input_values:
        to_return.append((i, 1))
    
    return to_return

def reduce_function(input_tuples):
    '''
    reduce_function takes as input a list of key-value pairs, and returns as output anything it wants
    '''
    
    value_count_d = dict()
    for tuple_r in input_tuples:
        key = tuple_r[0]
        value = tuple_r[1]
        
        if key in value_count_d:
            value_count_d[key] += value
        else:
            value_count_d[key] = value
    
    for key in value_count_d:
        print(key)
        
    return value_count_d


def main():
    input_m = [1, 2, 3, 5,6,7, 8, 5,3, 4, 3, 4, 3, 4, 3, 4, 3, 4, 3, 4, 5, 6, 7, 8, 8, 4, 5, 4, 3, 4, 3, 4, 3, 4,2, 6, 7, 8, 5, 4, 3, 4, 4, 4, 4]
    n_mappers = 4
    n_reducers = 1
    
    mr_manager = MapReduceManager(map_function, reduce_function, input_m, n_mappers, n_reducers)
    mr_manager_process = multiprocessing.Process(target = mr_manager)
    mr_manager_process.start()
    mr_manager_process.join()
    
    pass

if __name__=="__main__":
    main()
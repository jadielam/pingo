'''
Created on Nov 23, 2014

@author: jadiel
'''

from tasksgraph.callableobject import AbstractTask

class MapClass(AbstractTask):
    
    def __call__(self):
        
        map_function = self.input_args['m_function']
        input_m = self.input_args['input_m']
        
        return map_function(input_m)
    
class ReduceClass(AbstractTask):
    
    def __call__(self):
        print("In ReduceClass")
        reduce_function = self.input_args['r_function']
        input_r = self.input_args['input_r']
        
        return reduce_function(input_r)

class IntermediateReduceClass(AbstractTask):
    pass

class ShuffleClass(AbstractTask):
    
    def __call__(self):
        '''
        I receive as input all the lists of tuples from the mappers
        I count the number of different keys to determine how to divide the load among reducers
        I send each tuple to the corresponding reducer and create the reducers to run
        '''
        print("In ShuffleClass")
        r_function = self.input_args['r_function']
        n_reducers = self.input_args['n_reducers']
        
        
        if n_reducers <= 1:
            r_input =  list()
            for output_list in self.parents_output:
                r_input.extend(output_list)
            input_args_r = dict()
            input_args_r['input_r'] = r_input
            input_args_r['r_function'] = r_function
            self.create_task(ReduceClass, [], input_args_r)
        
        else:
            
            keys = set()
            for output_list in self.parents_output:
                for r_tuple in output_list:
                    keys.add(r_tuple[0])
            
            n_keys = len(keys)
            del keys
                        
            keys_reducer_d = dict()         #contains the reducer to which each key is assigned
            r_input_lists=list()
            for i in range(n_reducers):
                r_input_lists.append(list())
            
            #Assigning each tuple to its corresponding reducer. The last reducer will have more tuples than everyone else.
            current_reducer = 1
            current_reducer_tuples = 0
            for output_list in self.parents_output:
                for r_tuple in output_list:
                    key = r_tuple[0]
                    if key in keys_reducer_d:
                        r_input_lists[keys_reducer_d].append(r_tuple)
                    else:
                        #If we need to add this key to a different reducer:
                        if current_reducer_tuples > n_keys/n_reducers and current_reducer < n_reducers:
                            current_reducer += 1
                            keys_reducer_d[key] = current_reducer
                            r_input_lists[current_reducer].append(r_tuple)
                            current_reducer_tuples = 1
                        else:
                            keys_reducer_d[key] = current_reducer
                            r_input_lists[current_reducer].append(r_tuple)
                            current_reducer_tuples += 1
                            
            #Sending the reducers to work
            for i in range(n_reducers):
                input_args_r = dict()
                input_args_r['r_function'] = r_function
                input_args_r['input_r'] = r_input_lists[i]
                
                self.create_task(ReduceClass, [], input_args_r)
                            
class MapReduce(AbstractTask):
    
    def __chunks(self, li, n):
        '''
        Returns a list of n lists that constitute a partition of list li
        
        '''
        if n < 1:
            n = 1
        return [li[i:i + n] for i in range(0, len(li), n)]
    
    def __call__(self):
        
        #TODO: This is a very simple Map-Reduce, with no sophisticated performance gains.
        #1. Divide the input fairly among mappers
        partition = self.__chunks(self.input_args['input_m'], self.input_args['n_mappers'])
        mappers = list()
        
        #2. Creating the mappers tasks
        for input_p in partition:
            input_map_d = dict()
            input_map_d['m_function'] = self.input_args['m_function']
            input_map_d['input_m'] = input_p
            mappers.append(self.create_task(MapClass, [], input_map_d))
            
        #3. Shuffler
        #creating the input to the shuffler task, because it will be the task that will create the reducer task
        s_input = dict()
        print(len(mappers))
        s_input['r_function'] = self.input_args['r_function']
        s_input['n_reducers'] = self.input_args['n_reducers']
        self.create_task(ShuffleClass, mappers, s_input)
        
        
import multiprocessing
import threading
from tasksgraph.coreengine import CoreEngine
    
class MapReduceManager:
    def __init__(self, m_function, r_function, input_m, n_mappers,  n_reducers=1, ir_function = None, synchronization_file = None):
        self.input_values = dict()
        self.input_values['m_function'] = m_function
        self.input_values['r_function'] = r_function
        self.input_values['n_mappers'] = n_mappers
        self.input_values['n_reducers'] = n_reducers
        self.input_values['ir_function'] = ir_function
        self.input_values['input_m'] = input_m
        self.__synchronization_file = synchronization_file
           
    def __call__(self):
        
        manager = multiprocessing.Manager()
        
        queue = manager.Queue()
        
        queue.put((MapReduce, 'root', self.input_values, None))
        
        core_engine_thread = threading.Thread(target=CoreEngine(max([self.input_values['n_mappers'], self.input_values['n_reducers']]), queue, manager, [], self.__synchronization_file))
        core_engine_thread.start()
        
        queue.join()
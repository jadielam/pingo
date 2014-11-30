'''
Created on Nov 29, 2014

@author: jadiel
'''

'''
The iterative model that this computations follow:
input_data
current = initial guess at the result

do{

    prev = current;
    current = [];
    
    for (chunk in input_data){
    
        curr += process_chunk(chunk, prev);
        
    }
     
     
} while (! is_converged(current, prev)
'''

from tasksgraph.callableobject import AbstractTask
from utilities.list_utilities import chunks
import pickle

class OutputTask(AbstractTask):
    '''
    Writes the final result of the computation to a file.
    '''
    
    def __call__(self):
        to_output = self.input_args[0]
        output_file = self.input_args[1]
        
        with open(output_file, "w+") as f:
            pickle.dump(to_output, f)

class MapConvergeTask(AbstractTask):
    def __call__(self):
        current_chunk = self.input_args[0]
        previous_chunk = self.input_args[1]
        is_converged_function = self.input_args[2]
        
        return is_converged_function(current_chunk, previous_chunk)
    

class ReduceConvergeTask(AbstractTask):
    def __call__(self):
        original_inputs = self.parents_output[0]
        parents_output = self.parents_output[1:]
        current = self.input_args
                
        for converged in parents_output:
            if not converged:
                
                self.create_task(LoopTask, [original_inputs['root_task_id'], self.task_id], (current))
                return False
        
        #If we reach this stage of the function, it means that everything converged, so fire the output function        
        self.create_task(OutputTask, [], (current, original_inputs['output_file']))
        
class ConvergeTask(AbstractTask):
    
    def __call__(self):
        original_inputs = self.parents_output[0]
        converge_inputs = self.parents_output[1]
        
        if (converge_inputs['converge'] != None):
            return converge_inputs['converge']
        else:
            converge_function = original_inputs['is_converged_function']
            current = converge_inputs['current']
            previous = converge_inputs['previous']
                        
            #TODO: Check if the parallelization that I am doing here is the fastest one,
            #or if I should design one that instead of creating the chunks, it simply passes
            #a reference of the entire lists to the is_converged_function, together with
            #the indexes from where it will start working.
            #Otherwise return false.
            current_chunks = chunks(current, original_inputs['n_processes'])
            previous_chunks = chunks(previous, original_inputs['n_processes'])
            
            if len(current_chunks) == len(previous_chunks):
                map_converge_tasks = list()
                
                for i in len(current_chunks):
                    current_chunk = current_chunks[i]
                    previous_chunk = previous_chunks[i]
                    
                    map_converge_tasks.append(self.create_task(MapConvergeTask, [], (current_chunk, previous_chunk, converge_function)))
                
                
                reduce_converge_task_id = self.create_task(ReduceConvergeTask, [original_inputs['root_task_id']].extend(map_converge_tasks), current)
                
                
            else:
                result = converge_function(current, previous)
                if not result:
                    self.create_task(LoopTask, [original_inputs['root_task_id']], current)
                    return False
                    
                else:
                    #output the results to file
                    self.create_task(OutputTask, [], (current, original_inputs['output_file']))
                    return True
        
class ProcessChunkTask(AbstractTask):
    
    def __call__(self):
        original_inputs = self.parents_output[0]
        process_chunk_function = original_inputs['process_chunk_function']
        chunk = self.input_args[0]
        previous = self.input_args[1]
        return process_chunk_function(chunk, previous)

class LoopTask(AbstractTask):
    
    def __call__(self):
        original_inputs = self.parents_output[0]
        input_data_partition = original_inputs['input_data_partition']
        current = self.input_args
        
        previous = current
        
        process_chunk_tasks = list()
        for chunk in input_data_partition:
            process_chunk_tasks.append(self.create_task(ProcessChunkTask, [original_inputs['root_task_id']], (chunk, previous)))
        
        self.create_task(UpdateTask, [original_inputs['root_task_id']].extend(process_chunk_tasks), previous)
        
class UpdateTask(AbstractTask):
    
    def __call__(self):
        original_inputs = self.parents_output[0]
        temporary_currents = self.parents_output[1:]
        previous = self.input_args
        update_function = original_inputs['update_function']
        current = update_function(temporary_currents)
        
        converge_inputs = dict()
        converge_inputs['previous'] = previous
        converge_inputs['current'] = current
        converge_inputs['converge'] = None
        
        self.create_task(ConvergeTask, [original_inputs['root_task_id'].append(self.task_id)], None)
        return converge_inputs

class IterativeComputation(AbstractTask):
    
   
    def __call__(self):
        input_data = self.input_args['input_data']
        current = self.input_args['initial_guess']
        input_data_partition = chunks(input_data, self.input_args['n_processes'])
        self.input_args['input_data_partition'] = input_data_partition
        self.input_args['root_task_id'] = self.task_id
        
        converge_inputs = dict()
        converge_inputs['converge'] = False
        converge_inputs['current'] = current
        converge_inputs['previous'] = None
        
        self.create_task(ConvergeTask, [self.task_id], None)
        return [self.input_args, converge_inputs]


import multiprocessing
import threading
from tasksgraph.coreengine import CoreEngine

class IterativeComputationManager:
    def __init__(self, process_chunk_function, is_converged_function, n_processes, input_data, initial_guess, output_file, update_function, synchronization_file = None):
        self.input_values = dict()
        self.input_values['process_chunk_function'] = process_chunk_function
        self.input_values['is_converged_function'] = is_converged_function
        self.input_values['n_processes'] = n_processes
        self.input_values['update_function'] = update_function
        self.input_values['input_data'] = input_data
        self.input_values['initial_guess'] = initial_guess
        self.input_values['output_file'] = output_file
        self.__synchronization_file = synchronization_file
        
    def __call__(self):
        
        manager = multiprocessing.Manager()
        
        queue = manager.Queue()
        
        queue.put((IterativeComputation, 'root', self.input_values, None))
        
        core_engine_thread = threading.Thread(target=CoreEngine(self.input_values['n_processes'], queue, manager, [], self.__synchronization_file))
        
        core_engine_thread.start()
        
        queue.join()
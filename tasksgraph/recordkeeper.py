'''Created on Oct 6, 2014

@author: jadiel
'''

from tasksgraph import TaskGraph
import multiprocessing
from collections import OrderedDict
from os import linesep


def write_function(output_task_args, parents_output, task_id):
    import time
    import pickle
    '''
    This function writes the output to file, together with some extra metadata
    '''
    
    file_name=output_task_args['file_name']
    output_task_id=output_task_args['task_id']   #Note that task_id is the id of the output task, and output_task_id is the id of the task that produced the output
    parent_ids=output_task_args['task_id']
    current_date=time.strftime("%d/%m/%Y %H:%M:%S")
    metadata=(output_task_id, parent_ids, current_date)
        
    
    if len(parents_output)>1:
        raise Exception("More than one parent")
    
    for p_output in parents_output:
        with open(file_name, 'wb') as output:
            pickle.dump((metadata, p_output), output, pickle.HIGHEST_PROTOCOL)
            
    taskid_filepath=output_task_args['taskid_filepath']
    queue_of_entries=output_task_args['queue_of_entries']
    taskid_filepath[output_task_id]=file_name
    queue_of_entries.put((output_task_id, file_name))
    
def read_function(input_task_args, parents_output, task_id):
    
    import pickle
    file_name=input_task_args['file_name']
    
    with open(file_name, 'rb') as f:
        content=pickle.load(f)
        
    
    
    #content[0] is metadata
    #content[1] is the output from the parent.
    return content[1]

def update_function(update_task_args, parents_output, task_id):
    
    q=update_task_args['queue_of_entries']
    file_path=update_task_args['file_path']
    if 'force' in update_task_args:
        force=update_task_args['force']
    else:
        force=False
    
    l=list()
    
    if q.qsize()>3 or force:
        while not q.empty():
            l.append(q.get())
    
    to_write=linesep.join([str(a) for a in l])
                
    with open(file_path, "a") as f:
        f.write(to_write+linesep)

def read_keeper_path(filepath, dictionary):
    try:
            
        #TODO: handle exceptions here, inside of the loop, so as to 
        #maximize the number of elements that I would add to the self.__key_value_odict
        
        with open(filepath, "r") as f:
            text=f.read()
        
        lines=text.split(linesep)
        
        key_value_tuples_l=[eval(a) for a in lines if a!='']
        
        for a in range(len(key_value_tuples_l)):
            
            key=key_value_tuples_l[a][0]
            value=key_value_tuples_l[a][1]
            dictionary[key]=value
                            
    except:
        pass
              
class RecordKeeper:
    
    def __init__(self, pool_size, keeper_path):
        self.taskgraph=TaskGraph(pool_size)
        self.keeper_path=keeper_path
        self.taskid_filepath=multiprocessing.Manager().dict()
        self.queue_of_entries=multiprocessing.Manager().Queue()
        read_keeper_path(self.keeper_path, self.taskid_filepath)
        #self.taskid_lock=multiprocessing.Lock()
    
    def __build_task_output_path(self, task_id):
        return str(self.keeper_path)+"_"+str(task_id)
    
    def create_task(self, parent_ids, input_value, user_function, context=None):
        
        #TODO: We are placing this lock here because of the SimpleContextManager.
        #we are placing it as precaution, but is not needed. If it were needed, it would
        #be a sign of programming by the user violating the assumptions
        #But we have it anyways, as a sign of safety.
        
        #Why a lock is not needed using NestedContextManager:
        #1. because for a given context I only modify the value of the entry related to that
        #   context in the map.  And it is assumed that there is a one-to-many relationship
        #   between processes and context (a process can run many contexts in its lifetime. 
        #   A context is only ran by one process.
        #2. Because it is assumed that I am not using multiprocessing to create tasks within a 
        #   given context.
        #self.taskid_lock.acquire()
        next_task_id=self.taskgraph.pick_next_task_id(context)
        #print(str(self.task_taskoutputfile_keeper))
        #If this computation has already being performed, then read it from file instead
        if next_task_id in self.taskid_filepath:            
            input_task_args=dict()
            input_task_args['file_name']=self.taskid_filepath[next_task_id]
            task_id=self.taskgraph.create_task(parent_ids, input_task_args, read_function)
            #self.taskid_lock.release()
        
        #Else perform the computation and create task that will write output to file.
        else:
            #self.taskid_lock.release()
            
            #1. Create the user function task
            task_id=self.taskgraph.create_task(parent_ids, input_value, user_function, context)
            
            #2. Create the write output task
            output_task_args=dict()
            output_task_args['file_name']=self.__build_task_output_path(task_id)
            output_task_args['task_id']=task_id
            output_task_args['parent_ids']=parent_ids
            output_task_args['taskid_filepath']=self.taskid_filepath
            output_task_args['queue_of_entries']=self.queue_of_entries
            write_task=self.taskgraph.create_task([task_id], output_task_args, write_function)
            
            #3. Create the update output dictionary task.
            update_task_args=dict()
            update_task_args['queue_of_entries']=self.queue_of_entries
            update_task_args['file_path']=self.keeper_path
            self.taskgraph.create_task([write_task], update_task_args, update_function)
        
        return task_id
    
    def join(self):
        self.taskgraph.join()
        
        #Force the last elements of the queue to be written to file
        update_task_args=dict()
        update_task_args['queue_of_entries']=self.queue_of_entries
        update_task_args['file_path']=self.keeper_path
        update_task_args['force']=True
        update_function(update_task_args, None, None)
        return
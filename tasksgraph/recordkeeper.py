'''Created on Oct 6, 2014

@author: jadiel
'''

from tasksgraph import TaskGraph
import multiprocessing
from collections import OrderedDict
from os import linesep
from multiprocessing.managers import BaseManager


class RecordKeeperDictionary:
    def __init__(self, synchronized_file, top_limit=3):
        self.__key_value_odict=OrderedDict()
        self.__last_written_element=-1  #index begins at zero
        self.__synchronized_file=synchronized_file
        
        #updates the dictionary with elements 
        self.__read_synchronized_file()
        
        
        #the top limit is the number of elements that I will allow to be
        #added to the dictionary before synchronizing the content of the 
        #dictionary with the content of the file.
        try:
            self.__top_limit=int(top_limit)
        except:
            self.__top_limit=3
        
        self.__addition_lock=multiprocessing.Lock()
        self.__synchronization_lock=multiprocessing.Lock()
    
    def __read_synchronized_file(self):
        
        try:
            
            #TODO: handle exceptions here, inside of the loop, so as to 
            #maximize the number of elements that I would add to the self.__key_value_odict
            
            with open(self.__synchronized_file, "r") as f:
                text=f.read()
            
            lines=text.split(linesep)
            
            key_value_tuples_l=[eval(a) for a in lines if a!='']
            
            for a in range(len(key_value_tuples_l)):
                
                key=key_value_tuples_l[a][0]
                value=key_value_tuples_l[a][1]
                self.__key_value_odict[key]=value
                                
                
        except:
            pass
        
    def __str__(self):
        key_value_l=list()
        for key in self.__key_value_odict.keys():
            key_value_l.append((key, self.__key_value_odict[key]))
        
        return ", ".join(key_value_l)
    
    def __repr__(self):
        key_value_l=list()
        for key in self.__key_value_odict.keys():
            key_value_l.append((key, self.__key_value_odict[key]))
        
        return ", ".join(key_value_l)
    
    def contains(self, key):
        return key in self.__key_value_odict
                
    def add_key_value(self, key, value):
        '''
        If the key is already present, it removes it and adds it again.
        The reason for this is the algorithm that we are using to write to file:
        We only write to file from the last written element of the ordered dictionary.
        If we simply update the value of this key instead of first deleting it, then the
        value will not be added to the end of the ordered dictionary, and the update
        will not be reflected in the file.
        
        In the file we follow the convention that if a key appears multiple times, then
        the last one is the valid one.
        '''
        
        with self.__addition_lock:
            if key in self.__key_value_odict:
                del self.__key_value_odict[key]
                self.__last_written_element-=1
                self.__key_value_odict[key]=value
            else:
                self.__key_value_odict[key]=value
            
            
    def synchronize_with_file(self, force=False):
        
        #0. Check that the number of elements that need to be written is enough
        #as a rule of thumb, flows with long computations should write frequently.
        #process with short computations should write less frequently.
        #argument force forces the synchronization to happen regardless of the value of
        #self.__top_limit
        
        with self.__synchronization_lock:
            if len(self.__key_value_odict)-self.__last_written_element>self.__top_limit or force:
            #1. collect the new tuples to be written
            
                key_value_tuples_l=list()
                i=0
                for key in self.__key_value_odict.keys():
                    if i>self.__last_written_element:
                        key_value_tuples_l.append((key, self.__key_value_odict[key]))
                    i+=1
                
                
                #2. Make them a string and append them to file.
                to_write=linesep.join([str(a) for a in key_value_tuples_l])
                
                with open(self.__synchronized_file, "a") as f:
                    f.write(to_write+linesep)
                
                self.__last_written_element=len(self.__key_value_odict)-1
                

class RecordKeeperManager(BaseManager):
    pass

RecordKeeperManager.register('RecordKeeperDictionary', RecordKeeperDictionary)

                
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
        
    print("Writing to file "+file_name)
    if len(parents_output)>1:
        raise Exception("More than one parent")
    
    for p_output in parents_output:
        with open(file_name, 'wb') as output:
            pickle.dump((metadata, p_output), output, pickle.HIGHEST_PROTOCOL)
            
    task_taskoutputfile_dict=output_task_args['task_taskoutputfile_keeper']
    
    task_taskoutputfile_dict.add_key_value(output_task_id, file_name)
    
    
def read_function(input_task_args, parents_output, task_id):
    
    import pickle
    file_name=input_task_args['file_name']
    
    with open(file_name, 'rb') as f:
        content=pickle.load(f)
        
    
    
    #content[0] is metadata
    #content[1] is the output from the parent.
    return content[1]

def update_function(update_task_args, parents_output, task_id):
    
    keeper_map=update_task_args['dictionary']
    keeper_map.synchronize_with_file()
    

  
class RecordKeeper:
    
    def __init__(self, pool_size, keeper_path):
        self.taskgraph=TaskGraph(pool_size)
        self.keeper_path=keeper_path
        self.rManager=RecordKeeperManager()
        self.rManager.start()
        self.task_taskoutputfile_keeper=self.rManager.RecordKeeperDictionary(self.keeper_path)
        
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
        if self.task_taskoutputfile_keeper.contains(next_task_id):
            
            input_task_args=dict()
            input_task_args['file_name']=self.__build_task_output_path(next_task_id)
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
            output_task_args['task_taskoutputfile_keeper']=self.task_taskoutputfile_keeper
            write_task=self.taskgraph.create_task([task_id], output_task_args, write_function, "RecordKeeper")
            
            #3. Create the update output dictionary task.
            update_task_args=dict()
            update_task_args['dictionary']=self.task_taskoutputfile_keeper
            self.taskgraph.create_task([write_task], update_task_args, update_function, "RecordKeeper")
        
        return task_id
    
    def join(self):
        self.taskgraph.join()
        self.task_taskoutputfile_keeper.synchronize_with_file(True)
        return
'''
Created by Jadiel
October 15, 2014
'''
from abc import ABCMeta, abstractmethod

class AbstractTask(object):
    __metaclass__=ABCMeta
    
    def __init__(self, task_id, input_args):
        self.task_id=task_id
        self.input_args=input_args
        
        #This two cannot be private, because they will be assigned by the CoreEngine just before
        #the object is assigned to a process.
        self.queue=None
        self.pipe_conn=None
        self.condition=None
        self.parents_output=None
        self.output=None
        
    @abstractmethod
    def __call__(self):
        '''
        The code in this function will be executed when the object is assigned to
        a process in the pool of processes. 
        
        The user of this function does not need to access 
        '''
        pass
    
    def getTask_id(self):
        return self.task_id
    
    def getInput_args(self):
        return self.input_args
        
    def __get_next_id(self):
        '''
        Requests the pipe to get from the CoreEngine what will be the next id that will
        be assigned to this context (task_id)
        '''
        self.condition.acquire()
        self.pipe_conn.send(self.task_id)
        self.condition.notify()
        self.condition.release()
        next_id=self.pipe_conn.recv()
        return next_id
        
    def create_task(self, taskclass, parent_ids, input_args):
        '''
        Creates a new task that executes the code that is implemented in taskclass, 
        , and sends this task together with its parent_ids down the queue
        so that the CoreEngine stores the task in his engine.
        
        
        '''
        new_task_id=self.__get_next_id()
        self.queue.put((taskclass(new_task_id, input_args), parent_ids))
        return new_task_id
    
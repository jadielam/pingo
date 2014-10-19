'''
Created on Oct 15, 2014

@author: jadiel
'''

from abc import ABCMeta, abstractmethod

class AbstractTask(object):
    __metaclass__=ABCMeta
    
    def __init__(self):
        self.queue=None
        self.parents_output=None
        
    def __close_connections(self, other_function):
        def inner(self):
            print("in __close_connections")
            other_function()
        return inner
    

    @abstractmethod
    def __call__(self):
        pass
    
    
    
    def create_task(self, taskclass):
        print("in create_task")
        self.queue.put(taskclass())
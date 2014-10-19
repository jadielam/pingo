'''
Created on Oct 8, 2014

@author: jadiel
'''

from abc import ABCMeta, abstractmethod

class ContextManager(object):
    '''
    A Context Manager returns what is the next task_id that most be assigned in an specific context
    Note that a task_id and a context_id are the same thing.  The context_id of a task B is the task_id of the
    task A on whose context task B was created. Note that task A does not necessarily needs to be one of
    tasks B parents.  
    '''
    __metaclass__=ABCMeta
    
    @abstractmethod
    def get_next_task_id(self, context=None):
        '''
        The function returns the next_task_id to be assigned automatically incrementing the task_id
        by one.
        '''
        pass
    
    @abstractmethod
    def pick_next_task_id(self, context=None):
        '''
        Returns what would be the next task id assigned to that context
        '''
        pass

class NestedContextManager(ContextManager):
    '''
    The NestedContextManager assumes that there is more than one context, that is, that tasks can be created
    at run time within other tasks. In order to be able to keep track of the state of a computation
    in such an environment, a NestedContextManager is needed.
    '''
    def __init__(self):
        self.contexts_map=dict()
        
    def get_next_task_id(self, context=None):
        '''
        If the context exists, increments one to the last position in the context, otherwise it creates
        a new context, and begins the count from 1.
        '''
        if context in self.contexts_map:
            last_task_id=self.contexts_map[context][len(self.contexts_map[context])-1]
            next_task_id=(context, last_task_id[1]+1)
            self.contexts_map[context].append(next_task_id)
            return next_task_id
        else:
            context_list=list()
            next_task_id=(context, 1)
            context_list.append(next_task_id)
            self.contexts_map[context]=context_list
            return next_task_id
        
    def pick_next_task_id(self, context=None):
        '''
        Returns the id assigned to the next task in that context without setting it in the map.
        '''
        if context in self.contexts_map:
            last_task_id=self.contexts_map[context][len(self.contexts_map[context])-1]
            next_task_id=(context, last_task_id[1]+1)
            return next_task_id
        else:
            return (context, 1)
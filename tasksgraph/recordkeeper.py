'''
Created on Oct 6, 2014

@author: jadiel
'''

from tasksgraph import TaskGraph

class RecordKeeper:
    
    def __init__(self, pool_size, keeper_path):
        self.taskgraph=TaskGraph(pool_size)
        self.keeper_path=keeper_path
        
    def create_task(self, parent_ids, input_value, user_function):
        pass
        
        
        
        
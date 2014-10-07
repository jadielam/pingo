'''
Created on Oct 2, 2014

@author: jadiel
'''

import multiprocessing
import logging
from multiprocessing import Pool


def task_function(user_function, input_args, parents_output, task_id, task_factory):
    def inner():
        return user_function(input_args, parents_output, task_id, task_factory), task_id
    return inner

class TaskGraph:
    
    next_id=0;
    
    def __init__(self, pool_size):
        self.workers=Pool(pool_size)
        self.id_task_dict=dict()
        self.pending_tasks=0
    
    def id_in_graph(self, ids):
        '''
        Return True if there is at least one id in the list of ids that is in the id_task_dict.
        Otherwise it returns false
        '''
        if ids==None: return False
        for task_id in ids:
            if task_id in self.id_task_dict:
                return True
        return False
    
    
    def task_finished(self, task_id):
        
        def inner(output_result):
            
            if task_id in self.id_task_dict:
                self.pending_tasks-=1
                task=self.id_task_dict[task_id]
                task.setOutput(output_result)
                
                
                for child in task.getChildren():
                    
                    child.father_finished(task_id)
                    
                    if child.are_parents_done():
                        
                        input_args=task.getInput_args()
                        
                        parents_output=[father.getOutput() 
                                        for father in child.getParents()]
                        
                        user_function=child.getUser_function()
                        self.workers.apply_async(func=user_function,
                                                  args=[input_args,
                                                        parents_output,
                                                        child.getId()],
                                                  callback=self.task_finished(child.getId())
                                                  )
                        
        return inner
        
        
    def create_task(self, parent_ids, input_value, user_function):
        TaskGraph.next_id+=1;
        this_task_id=TaskGraph.next_id
        
        
        #1. Finding the list of parents to a task and updating the graph with both children and parents
        if parent_ids==None:
            task=TaskGraph.Task(this_task_id, None, input_value, user_function)
        else:
            parents=list()
            for father_id in parent_ids:
                if father_id in self.id_task_dict:
                    parents.append(self.id_task_dict[father_id])
                    
            task=TaskGraph.Task(this_task_id, parents, input_value, user_function)
            
            for father in parents:
                father.addChild(task)
                
                
                
        if not self.id_in_graph(parent_ids):
            input_args=task.getInput_args()
            user_function=task.getUser_function()
            self.workers.apply_async(func=user_function, 
                                     args=[input_args, 
                                           None, 
                                           this_task_id],
                                     callback=self.task_finished(this_task_id)
                                     )
                            
            #else if task is dependent
        else:
                #if the parents are done.
            if task.are_parents_done():
                input_args=task.getInput_args()
                parents_output=[self.id_task_dict[father_id].getOutput() 
                                for father_id in parent_ids
                                if father_id in self.id_task_dict]
                user_function=task.getUser_function()
                self.worskers.apply_async(func=user_function,
                                          args=[input_args,
                                                parents_output,
                                                this_task_id
                                                ],
                                          callback=self.task_finished(this_task_id)
                                          )
                
        self.id_task_dict[this_task_id]=task
        self.pending_tasks+=1 
        return task.getId()

    
    def join(self):
        while self.pending_tasks>0:
            pass
        
        self.workers.close()
        self.workers.join()
    
    class Task:
        def __init__(self, task_id, parents, input_args, user_function):
            self.id=task_id
            self.parents=parents
            self.input_args=input_args
            self.user_function=user_function
            self.children=list()
            self.output=None
            self.ids_of_parents_done=set()
            
        def are_parents_done(self):
            if self.parents==None: return True
            if len(self.ids_of_parents_done)==len(self.parents):
                return True
            return False
        
        def father_finished(self, father_id):
            self.ids_of_parents_done.add(father_id)
            
        def getId(self):
            return self.id
            
        def getChildren(self):
            return self.children
        
        def addChild(self, child):
            if (isinstance(child, TaskGraph.Task)):
                self.children.append(child)
            
        def getParents(self):
            return self.parents
        
        def getInput_args(self):
            return self.input_args
        
        def getUser_function(self):
            return self.user_function
        
        def setOutput(self, output):
            self.output=output
            
        def getOutput(self):
            return self.output       
        

   
'''
Created on Oct 2, 2014

@author: jadiel
'''

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
                task=self.id_task_dict[task_id]
                task.setOutputResult(output_result)
                for child in task.getChildren():
                    child.father_finished(task_id)
                    if child.are_parents_done():
                        input_args=task.getInput_args()
                        
                        parents_output=[father.getOutput() 
                                        for father in child.getParents()]
                        user_function=task.getUser_function()
                        self.worskers.apply_async(func=task_function,
                                                  args=[user_function,
                                                        input_args,
                                                        parents_output,
                                                        child.getId(),
                                                        self.create_task],
                                                  callback=self.task_finished(child.getId())
                                                  )
        return inner
        
    def add_to_graph_and_workers(self, factory_function):
        '''
        Adds the task to graph and send task to workers if task is independent or if parents
        of task are already done.
        
        '''
        #TODO: I have to implement the synchronization of this function.
        #I wish that python had a simple word such as java synchronized.
        def inner(self, parent_ids, input_value, user_function):
            TaskGraph.next_id+=1;
            task_id=TaskGraph.next_id
            
            task=factory_function(parent_ids, input_value, user_function)
            self.id_task_dict[task.getId()]=task
            
            #if task is independent
            if not self.id_in_graph(parent_ids):
                input_args=task.getInput_args()
                user_function=task.getUser_function()
                self.workers.apply_async(func=task_function, 
                                         args=[user_function, 
                                               input_args, 
                                               None, task_id,
                                               self.create_task],
                                         callback=self.task_finished(task_id)
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
                    self.worskers.apply_async(func=task_function,
                                              args=[user_function,
                                                    input_args,
                                                    parents_output,
                                                    task_id,
                                                    self.create_task],
                                              callback=self.task_finished(task_id)
                                              )
                
               
            return task.getId()
                                                                  
        return inner
    
    @add_to_graph_and_workers
    def create_task(self, parent_ids, input_value, user_function):
        this_task_id=TaskGraph.next_id
        if parent_ids==None:
            return TaskGraph.Task(this_task_id, None, input_value, user_function)
        else:
            parents=list()
            for father_id in parent_ids:
                if father_id in TaskGraph.id_task_dict:
                    parents.append(TaskGraph.id_task_dict[father_id])
            return TaskGraph.Task(this_task_id, parents, input_value, user_function)
      
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
'''
Created on Oct 2, 2014
Solution is: whenever I add a new child to the parent,
I check if the parent is done already
and I call the parent_done method on the child.
@author: jadiel
'''
import multiprocessing
from multiprocessing import Pool

from exceptions import ConcatenatingException
from context import NestedContextManager

def task_function(user_function, input_args, parents_output, task_id, task_factory):
    '''
    Currently not used. This function is a wrapper that makes the user_function return a tuple
    instead of whatever else the user function would return.
    
    TODO: I am not using it because I have to figure out a way to handle variable sized arguments.
    '''
    def inner():
        return user_function(input_args, parents_output, task_id, task_factory), task_id
    return inner

def exception_function(input_args, parents_output, task_id):
    '''
    Function that receives as input an exception and generates a new exception that adds the 
    task id of the current exception to the previous exception and throws it.
    There will be an exception among one of the parents output if this function is called.
    '''
    new_message="Error in task "+str(task_id)+" because of a father malfunction"
    raise ConcatenatingException(parents_output, new_message)
    
    
class TaskGraph:
    '''
    Class to manage the creation and running of pool tasks.
    '''
       
    def __init__(self, pool_size):
        self.workers=Pool(pool_size)
        self.id_task_dict=dict()
        self.pending_tasks=0
        self.context_manager=NestedContextManager()
    
    def __id_in_graph(self, ids):
        '''
        Return True if there is at least one id in the list of ids that is in the id_task_dict.
        Otherwise it returns false
        '''
        if ids==None: return False
        for task_id in ids:
            if task_id in self.id_task_dict:
                return True
        return False
    
    def __task_aborted(self, task_id):
        '''
        Whenever a task is aborted, this method gets called
        '''
        def inner(exception):
            '''
            If a task is aborted, because of an exception, I will run all the children
            from the graph with a different function that reports the exception from the
            father and it also throws another exception, so that a chain reaction happens and 
            all the descendants of the original task are aborted.
            
            This is the most elegant solution I could come up with.
            '''    
            print("Exception: "+str(exception))
            
            if task_id in self.id_task_dict:
                self.pending_tasks-=1
                task=self.id_task_dict[task_id]
                task.setStateFailed()
                task.setDone()
                task.setOutput(str(exception))
                
                for child in task.getChildren():
                    child.father_finished(task_id)
                    child.setUser_function(exception_function)
                    
                    if child.are_parents_done():
                        input_args=child.getInput_args()
                        parents_output=[father.getOutput()
                                        for father in child.getParents()]
                        user_function=child.getUser_function()
                        
                        self.workers.apply_async(func=user_function,
                                                  args=[input_args,
                                                        parents_output,
                                                        child.getId()],
                                                  callback=self.__task_finished(child.getId()),
                                                  error_callback=self.__task_aborted(child.getId())
                                                  )
        return inner
    
    def __task_finished(self, task_id):
        
        def inner(output_result):
            
            if task_id in self.id_task_dict:
                self.pending_tasks-=1
                task=self.id_task_dict[task_id]
                task.setDone()
                task.setOutput(output_result)
                
                
                for child in task.getChildren():
                    
                    child.father_finished(task_id)
                    
                    if child.are_parents_done():
                        
                        input_args=child.getInput_args()
                        
                        parents_output=[father.getOutput() 
                                        for father in child.getParents()]
                        
                        user_function=child.getUser_function()
                        self.workers.apply_async(func=user_function,
                                                  args=[input_args,
                                                        parents_output,
                                                        child.getId()],
                                                  callback=self.__task_finished(child.getId()),
                                                  error_callback=self.__task_aborted(child.getId())
                                                  )
                        
        return inner
        
    def pick_next_task_id(self, context):
        
        return self.context_manager.pick_next_task_id(context)
        
    def create_task(self, parent_ids, input_value, user_function, context=None):
        this_task_id=self.context_manager.get_next_task_id(context)
                        
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
                
                
        #2. If the task is independent, or if all its parents         
        if not self.__id_in_graph(parent_ids):
            input_args=task.getInput_args()
            user_function=task.getUser_function()
            self.workers.apply_async(func=user_function, 
                                     args=[input_args, 
                                           None, 
                                           this_task_id],
                                     callback=self.__task_finished(this_task_id),
                                     error_callback=self.__task_aborted(this_task_id)
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
                self.workers.apply_async(func=user_function,
                                          args=[input_args,
                                                parents_output,
                                                this_task_id
                                                ],
                                          callback=self.__task_finished(this_task_id),
                                          error_callback=self.__task_aborted(this_task_id)
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
            self.done=False
            self.state="created"
            self.ids_of_parents_done=set()
        
        
        def setStateReady_to_run(self):
            self.state="ready-to-run"
            
        def setStateRunning(self):
            self.state="running"
        
        def setStateFinished(self):
            self.state="finished"
        
        def setStateFailed(self):
            self.state="failed"
                
        def isDone(self):
            return self.done
        
        def setDone(self):
            self.done=True
            
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
                if self.done:
                    child.father_finished(self.id)
                if self.state=="failed":
                    child.setUser_function(exception_function)
            
        def getParents(self):
            return self.parents
        
        def getInput_args(self):
            return self.input_args
        
        def setUser_function(self, user_function):
            self.user_function=user_function
            
        def getUser_function(self):
            return self.user_function
        
        def setOutput(self, output):
            self.output=output
            
        def getOutput(self):
            return self.output       
'''
Created on Oct 2, 2014
Solution is: whenever I add a new child to the parent,
I check if the parent is done already
and I call the parent_done method on the child.
@author: jadiel
'''
import multiprocessing
import threading
from multiprocessing import Pool


from tasksgraph.exceptions import ConcatenatingException
from tasksgraph.callableobject import AbstractTask
from tasksgraph.context import NestedContextManager



class IdAssignerTask(object):
    '''
    These are threads that receive requests through pipes
    to return the next available id for a given context.
    
    They contain a context object that helps them do the task.
    The context maps do not need to be shared among different
    pipetasks, because one context will always use the same 
    pipe to send its requests.
    
    We will define the IdAssignerTask thread as the consumer thread in the relationship.
    The AbstractTask threads will be the producers of requests.
    '''
    
    def __init__(self, pipe_conn, pipe_condition):
        self.__context_manager=NestedContextManager()
        self.__pipe_conn=pipe_conn
        self.__pipe_condition=pipe_condition
        
    
    def __call__(self):
        '''
        In order to finish executing the thread, just close the other end 
        of the connection, and it will finish executing.
        '''
        
        finish_flag=False
         
        while True:
            
            self.__pipe_condition.acquire()
            while True:
                try:
                    self.__pipe_condition.wait()
                except:
                    finish_flag=True
                    break
                
                #The recv call is blocking.
                #That is why we need a condition to make this efficient.
                context_id=self.__pipe_conn.recv()
                if context_id:
                    break
            
            if finish_flag:
                "closing pipe"
                self.__pipe_conn.close()
                break
            
            self.__pipe_condition.release()
            
            
            
            next_task_id=self.__context_manager.get_next_task_id(context_id)
            self.__pipe_conn.send(next_task_id)
            
class ExceptionTask(AbstractTask):
    def __call__(self):
        new_message="Error in task "+str(self.task_id)+" because of parent malfunction."
        raise ConcatenatingException(self.parents_output, new_message)
    
class Task:
    def __init__(self, task_id, parents, input_args, callable_object):
        self.id=task_id
        self.parents=parents
        self.input_args=input_args
        self.callable_object=callable_object
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
        if (isinstance(child, Task)):
            self.children.append(child)
            if self.done:
                child.father_finished(self.id)
            if self.state=="failed":
                child.setUser_function(ExceptionTask(child.getId(), child.getInput_args()))
        
    def getParents(self):
        return self.parents
    
    def getInput_args(self):
        return self.input_args
    
    def setUser_function(self, user_function):
        self.callable_object=user_function
        
    def getUser_function(self):
        return self.callable_object
    
    def setOutput(self, output):
        self.output=output
        
    def getOutput(self):
        return self.output           
    
class CoreEngine:
    '''
    Class to manage the creation and running of pool tasks.
    '''
       
    def __init__(self, pool_size, queue, manager, processors):
        
        self.__workers=Pool(pool_size)
        
        self.__queue=queue
        
        #used to create synchronization objects to pass to threads
        self.__manager=manager
        
        self.__id_task_dict=dict()
        
        self.__processors=processors
        
        #is incremented each time a task is sent to run, and decremented
        #each time it finishes or fails.
        self.__pending_tasks=0   
    
    def __call__(self):
        while not self.__queue.empty() or self.__pending_tasks>0:
            task, parent_ids=self.__queue.get()
                        
            self.__process_task(task, parent_ids)
            
            for processor in self.__processors:
                processor.process(task)
        
    
    def __id_in_graph(self, ids):
        '''
        Return True if there is at least one id in the list of ids that is in the __id_task_dict.
        Otherwise it returns false
        '''
        if ids==None: return False
        for task_id in ids:
            if task_id in self.__id_task_dict:
                return True
        return False
    
    def __process_task(self, user_function, parent_ids):
        '''
        This might be the function doing all the dirty job.
        
        '''
        #user_function is the same as the task received from the queue, just that I keep that name
        #in order to not break this code already written
        print("in process_task")
        
        
        this_task_id=user_function.getTask_id()
        input_value=user_function.getInput_args()
        print(this_task_id)                        
        #1. Finding the list of parents to a task and updating the graph with both children and parents
        if parent_ids==None:
            task=Task(this_task_id, None, input_value, user_function)
        else:
            
            #A check if it is that I get a single element instead of a list.
            if not isinstance(parent_ids, list):
                temp_parents=list()
                temp_parents.append(parent_ids)
                parent_ids=temp_parents
                
            parents=list()
            print(parent_ids)
            for father_id in parent_ids:
                if father_id in self.__id_task_dict:
                    parents.append(self.__id_task_dict[father_id])
                    
            task=Task(this_task_id, parents, input_value, user_function)
            
            for father in parents:
                father.addChild(task)
                
                
        #2. If the task is independent         
        if not self.__id_in_graph(parent_ids):
            
            self.__assign_task_to_process(task)
                            
        #else if task is dependent
        else:
            #if the parents are done.
            if task.are_parents_done():
                print("pparents_are_done")
                self.__assign_task_to_process(task)
        
        self.__id_task_dict[this_task_id]=task
        
        return task.getId()

    def __get_parent_ids(self, task):
        
        parents=task.getParents()
        if parents==None: return []
        
        parent_ids=[]
        
        for parent in parents:
            parent_ids.append(parent.getId())
        return parent_ids
    
    def __assign_task_to_process(self, task):
        
        user_function=task.getUser_function()
        
        #0. TODO: I don't need input_args in task
        
        #1. Set the parents_output
        print(user_function.getTask_id())
        parent_ids=self.__get_parent_ids(task)
        parents_output=[self.__id_task_dict[father_id].getOutput() 
                                for father_id in parent_ids
                                if father_id in self.__id_task_dict]
        user_function.parents_output=parents_output
            
        #2. Set the queue
        user_function.queue=self.__queue
                
        #3. Set the pipes and create the IdAssignerThread
        conn1, conn2=multiprocessing.Pipe()
        condition=self.__manager.Condition()
        user_function.condition=condition
        user_function.pipe_conn=conn1
        
        id_assigner_thread=threading.Thread(target=IdAssignerTask(conn2, condition))
        id_assigner_thread.start()
        
        self.__workers.apply_async(func=user_function,
                                   callback=self.__task_finished(user_function.getTask_id()),
                                   error_callback=self.__task_aborted(user_function.getTask_id())
                                  )
        self.__pending_tasks+=1
    
    
    def registerProcessor(self, processor):
        self.__processors.append(processor)

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
            
            if task_id in self.__id_task_dict:
                
                self.__pending_tasks-=1
                                
                task=self.__id_task_dict[task_id]
                #TODO: I need to call this automatically whenever that thread is done
                #I need to figure out how to cover the apply function with another function
                task.getUser_function().pipe_conn.close()
                task.setStateFailed()
                task.setDone()
                task.setOutput(str(exception))
                
                for child in task.getChildren():
                    child.father_finished(task_id)
                    child.setUser_function(ExceptionTask(child.getId(), child.getInput_args()))
                    
                    if child.are_parents_done():
                        self.__assign_task_to_process(child)
                
                self.__queue.task_done()
        return inner
    
    def __task_finished(self, task_id):
        
        def inner(output_result):
            
            if task_id in self.__id_task_dict:
                self.__pending_tasks-=1
                print("task_finished "+str(task_id))
                task=self.__id_task_dict[task_id]
                
                #TODO: I need to call this automatically whenever that thread is done
                #I need to figure out how to cover the apply function with another function
                task.getUser_function().pipe_conn.close()
                task.setDone()
                task.setOutput(output_result)
                               
                
                for child in task.getChildren():
                    
                    child.father_finished(task_id)
                    
                    if child.are_parents_done():
                        
                        self.__assign_task_to_process(child)
                
                self.__queue.task_done()        
        return inner
        

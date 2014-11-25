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
from tasksgraph.context import NestedContextManager
from tasksgraph.taskgraph import TaskGraph
from tasksgraph.systemtasks import ExceptionTask
from tasksgraph.systemtasks import OutputWriterTask

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
    
    def __init__(self, pipe_conn, pipe_condition, task_id):
        self.__context_manager=NestedContextManager()
        self.__pipe_conn=pipe_conn
        self.__pipe_condition=pipe_condition
        self.__task_id = task_id
    
    def __call__(self):
        '''
        In order to finish executing the thread, just close the other end 
        of the connection, and it will finish executing.
        '''
        
        while True:
            
            try:
                
                context_id=self.__pipe_conn.recv()
                if context_id:
                    next_task_id=self.__context_manager.get_next_task_id(context_id)
                    self.__pipe_conn.send(next_task_id)
                else:
                    break
            except:
                break
    
class CoreEngine:
    '''
    Class to manage the creation and running of pool tasks.
    '''
       
    def __init__(self, pool_size, queue, manager, processors, synchronization_file=None):
        
        self.__workers=Pool(pool_size)
        
        self.__queue=queue
        
        #used to create synchronization objects to pass to threads
        self.__manager=manager
        
        self.__task_graph=TaskGraph(synchronization_file)
        
        self.__processors=processors
        
        #is incremented each time a task is sent to run, and decremented
        #each time it finishes or fails.
        self.__pending_tasks=0   
        
        self.__context_manager=NestedContextManager()
        
        self.__run_lock=multiprocessing.Lock()
    
    def __call__(self):
        while True:
            try:
                (task_class, task_id, input_args, parent_ids)=self.__queue.get()
                                       
                self.__process_task(task_class, task_id, input_args, parent_ids)
                
                for processor in self.__processors:
                    processor.process(task_id)
            except EOFError:
                break
        
        self.__task_graph.synchronize(True)
    
    def __id_in_graph(self, ids):
        '''
        Return True if there is at least one id in the list of ids that is in the __id_task_dict.
        Otherwise it returns false
        '''
        if ids==None: return False
        for task_id in ids:
            if self.__task_graph.contains(task_id):
                return True
        return False
    
    def __finished(self, task_id):
        '''
        returns True if a task is in a dictionary and is already done
        Otherwise returns False
        '''
        
        return self.__task_graph.finished(task_id)
        
    def __process_task(self, task_class, this_task_id, input_value, parent_ids):
        '''
        This might be the function doing all the dirty job.
        
        '''        
        
        #If the task does not exist, or if the task exists but failed
        if not self.__task_graph.contains(this_task_id) or self.__task_graph.failed(this_task_id):
            
            #1. Finding the list of parents to a task and updating the graph with both children and parents
            
            if parent_ids==None:
                
                self.__task_graph.create_task(this_task_id, None, input_value, task_class)
                
            else:
                
                #A check if it is that I get a single element instead of a list.
                if not isinstance(parent_ids, list):
                    temp_parents=list()
                    temp_parents.append(parent_ids)
                    parent_ids=temp_parents
                
                self.__task_graph.create_task(this_task_id, parent_ids, input_value, task_class)   
                
                
           
            #1.1 Creating the output task to this task
            #Maybe I don't need this anymore since I am saving everything in the TaskGraph.
            #output_task_id=self.__context_manager.get_next_task_id("outputtask")
            #output_task_function=OutputWriterTask(output_task_id, this_task_id)
            #self.__task_graph.create_task(output_task_id, [this_task_id], this_task_id, output_task_function, "outputtask")
                                
            #2. If the task is independent         
            if not self.__id_in_graph(parent_ids):
                
                self.__assign_task_to_process(this_task_id)
                                
            #else if task is dependent
            else:
                #if the parents are done.
                if self.__task_graph.are_parents_done(this_task_id):
                    
                    self.__assign_task_to_process(this_task_id)
                    
            
            
        
        else:
            #If the task is already in task graph and marked as finished, then place all of its children in the queue
            #to be processed.
            '''
            TODO:
            Another source of error here.
            If a task is marked as done, then all its children will be placed in the queue.
            If I place a non-normal task in the queue, I could potentially have infinite
            recursion here (i.e.: An output task placed in the queue will be processed
            as a normal task and an output task will be created for it.
            
            Somehow I must develop better semantic in the program so as to determine
            what the behavior is for different kind of tasks.
            
            Temporary solution: Make the task graph return all its normal children, so that
            no hazzle is done here.
            
            NOTE: Error has been fixed using the temporary solution. 
            '''
            
            
            self.__queue.task_done()
            normal_children_ids=self.__task_graph.get_normal_children_ids(this_task_id)
            
            for child_id in normal_children_ids:
                
                if self.__task_graph.contains(child_id):
                    task_class=self.__task_graph.get_task_class(child_id)
                    input_args=self.__task_graph.get_input_args(child_id)
                    
                    parent_ids=self.__task_graph.get_parents(child_id)
                    
                    self.__queue.put((task_class, child_id, input_args, parent_ids))
                
        return this_task_id

    def __get_parent_ids(self, task_id):
        
        return self.__task_graph.get_parents(task_id)
            
    def __assign_task_to_process(self, task_id):
        
        
        self.__run_lock.acquire()
        if not self.__task_graph.is_running(task_id) and not self.__task_graph.finished(task_id):
            
            
            self.__task_graph.set_state_running(task_id)
            self.__run_lock.release()
                    
            callable_object=self.__task_graph.get_callable_object(task_id)
            
            #0. TODO: I don't need input_args in task
            
            #1. Set the parents_output
            
            #Here is where the magic happens.
            parents_output = self.__task_graph.get_parents_output(task_id)
            recorders_output = self.__task_graph.get_reporters_output(task_id)
            callable_object.parents_output=parents_output
            callable_object.recorders_output=recorders_output
                
            #2. Set the queue
            callable_object.queue=self.__queue
                    
            #3. Set the pipes and create the IdAssignerThread
            conn1, conn2=multiprocessing.Pipe()
            condition=self.__manager.Condition()
            callable_object.condition=condition
            callable_object.pipe_conn=conn1
            
            id_assigner_thread=threading.Thread(target=IdAssignerTask(conn2, condition, task_id))
            id_assigner_thread.start()
            
            self.__workers.apply_async(func=callable_object,
                                       callback=self.__task_finished(callable_object),
                                       error_callback=self.__task_aborted(callable_object)
                                      )
            
            
            self.__pending_tasks+=1
        
        else:
            self.__run_lock.release()
        
    
    
    def registerProcessor(self, processor):
        self.__processors.append(processor)

    def __task_aborted(self, callable_object):
        '''
        Whenever a task is aborted, this method gets called
        '''
        task_id=callable_object.getTask_id()
        
        def inner(exception):
            '''
            If a task is aborted, because of an exception, I will run all the children
            from the graph with a different function that reports the exception from the
            father and it also throws another exception, so that a chain reaction happens and 
            all the descendants of the original task are aborted.
            
            This is the most elegant solution I could come up with.
            '''    
            
            print("Exception: "+str(exception))
            
            if self.__task_graph.contains(task_id):
                
                self.__pending_tasks-=1
                
                #TODO: I need to call this automatically whenever that thread is done
                #I need to figure out how to cover the apply function with another function
                callable_object.pipe_conn.close()
                self.__task_graph.set_state_failed(task_id)
                #self.__task_graph.set_done(task_id)
                self.__task_graph.set_output(task_id, str(exception))
                
                for child_id in self.__task_graph.get_children_ids(task_id):
                    self.__task_graph.set_father_finished(child_id, task_id)
                    self.__task_graph.set_task_class(child_id, ExceptionTask, self.__task_graph.get_input_args(child_id))
                    print("got here")
                    if self.__task_graph.are_parents_done(child_id):
                        print("got here inside")
                        self.__assign_task_to_process(child_id)
                
                if self.__task_graph.get_task_type(task_id)=="normal":
                    self.__queue.task_done()
            print("finished here.")
        return inner
    
    def __task_finished(self, callable_object):
        
        task_id=callable_object.getTask_id()
        def inner(output_result):
            
            if self.__task_graph.contains(task_id):
                self.__pending_tasks-=1
                
                
                              
                #TODO: I need to call this automatically whenever that thread is done
                #I need to figure out how to cover the apply function with another function
                
                callable_object.pipe_conn.close()
                self.__task_graph.set_done(task_id)
                self.__task_graph.set_output(task_id, output_result)
                
                
                for child_id in self.__task_graph.get_children_ids(task_id):
                    
                    self.__task_graph.set_father_finished(child_id, task_id)
                                     
                    if self.__task_graph.are_parents_done(child_id):
                        self.__assign_task_to_process(child_id)
                
                if self.__task_graph.get_task_type(task_id)=="normal":
                                   
                    self.__queue.task_done()        
        
        return inner
        

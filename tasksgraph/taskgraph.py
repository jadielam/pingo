'''
Created on Oct 30, 2014

@author: jadiel
'''
import pickle

from tasksgraph.systemtasks import ExceptionTask

class Task:
    def __init__(self, task_id, parents, input_args, callable_object, ttype='normal'):
        self.id=task_id
        if parents == None:
            self.parents=list()
        else:
            self.parents=parents
            
        self.input_args=input_args
        self.callable_object=callable_object
        self.children=list()
        self.output=None
        self.done=False
        self.state="created"
        self.ids_of_parents_done=set()
        self.__type=ttype
        self.__output_file=None
    
    def setOutputFile(self, output_file):
        self.__output_file=output_file
        
    def getOutputFile(self):
        return self.__output_file
    
    def setStateReady_to_run(self):
        self.state="ready-to-run"
        
    def setStateRunning(self):
        self.state="running"
    
    def setStateFinished(self):
        self.state="finished"
    
    def setStateFailed(self):
        self.state="failed"
    
    def getState(self):
        return self.state
            
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
    
    def getType(self):
        return self.__type
        
    def getChildren(self):
        return self.children
    
    def addChild(self, child_id):
        self.children.append(child_id)
                    
    def getParents(self):
        return self.parents
    
    def getInput_args(self):
        return self.input_args
    
    def setCallable_object(self, user_function):
        self.callable_object=user_function
        
    def getCallable_object(self):
        return self.callable_object
    
    def setOutput(self, output):
        self.output=output
        
    def getOutput(self):
        return self.output           

class TaskGraph:
    
    def __init__(self, synchronization_file=None):
        self.__id_task_dict=dict()
        self.__to_synchronize=list()
        self.__synchronization_file=synchronization_file
    
    def __read_synchronization_file(self):
        '''
        Called at construction time to read the taskgraph from file.
        The idea is simple:
        
        1. The synchronization file is a list of tasks
        2. I add the tasks to the task graph in the order they appear in the synchronization file.
        2.1 If a task is already in the task graph, I overwrite it, because the new task
        will have the latest updates.
        3. Whenever I add a task to the task graph I have to connect parents and children in a 
        smart way: If a task was already in the task graph I need to be careful to not
        repeat that task in the lists maintained by either children or parents.
        NOTE on 3.: I actually don't have to connect parents and children together. Since they come 
        already connected. I do have to be careful that if I don't read a complete file because it is corrupted, 
        I need to be able to rollback the map to plain 
        '''
        try:
            f = open(self.__synchronization_file, "rb")
            while True:
                task = pickle.load(f)
                self.__id_task_dict[task.getId()] = task
        except EOFError:
            #do nothing, because this is what we were waiting for.
            f.close()
        except:
            print("File "+str(self.__synchronization_file)+" does not exist")
            #I do this because I don't want to have the map in an inconsistent state. 
            #I need to sit down and think of a better solution where I don't have to delete 
            #everything.
            self.__id_task_dict.clear()
            #TODO: If instead of doing this, I was simply connecting parents and children together in a careful way, as indicated in point 3.
            #Then I wouldn't have the need to clear here, because at any moment, the graph would be consistent.
    
    def synchronize(self, force=False):
        '''
        It writes to the synchronization file the elements in the synchronization list.
        This is a blocking method.
        TODO: Make it non-blocking 
        -When synchronizing a list we copy the elements of the __to_synchronize list to
        a temporary list and we clear the __to_synchronize list.  If the synchronizing task
        does not succeed, we prepend the elements of the temporary list back into the __to_synchronize
        list.
        '''
        
        if (len(self.__to_synchronize)>4) or force:
            synchronizing=self.__to_synchronize.copy()
            self.__to_synchronize.clear()
            
            tasks = [self.__id_task_dict[task_id] for task_id in synchronizing]
            try:
                f = open(self.__synchronization_file, "wb+")
                for task in tasks:
                    pickle.dump(task, f)
                f.close()
            except:
                # We take care of things this way anticipating some multi-hreading in this method.
                synchronizing.extend(self.__to_synchronize)
                self.__to_synchronize = synchronizing
            
    def contains(self, task_id):
        '''
        Returns True if the task is in the map, otherwise returns False
        '''
        return task_id in self.__id_task_dict
    
    def get_task_type(self, task_id):
        '''
        Returns the type (a string) of the task represented by task_id.
        Returns None if the task_id is not in the dictionary of tasks.
        '''
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].getType()
        return None
    
    def id_in_graph(self, ids):
        '''
        Return True if there is at least one id in the list of ids that is in the __id_task_dict.
        Otherwise it returns false
        '''
        if ids==None: return False
        for task_id in ids:
            if task_id in self.__id_task_dict:
                return True
        return False

    def finished(self, task_id):
        '''
        returns True if a task is in a dictionary and is already done
        Otherwise returns False
        '''
        
        if not task_id in self.__id_task_dict:
            return False
        return self.__id_task_dict[task_id].isDone()
    
        
    def create_task(self, task_id, parent_ids, input_args, callable_object, ttype="normal"):
        if parent_ids==None:
            task=Task(task_id, None, input_args, callable_object, ttype)
            self.__id_task_dict[task_id]=task
            self.__to_synchronize.append(task_id)
            self.synchronize()
        else:
            if not isinstance(parent_ids, list):
                temp_parents=list()
                temp_parents.append(parent_ids)
                parent_ids=temp_parents
             
            task=Task(task_id, parent_ids, input_args, callable_object, ttype)
            
            self.__id_task_dict[task_id]=task
            
            for father_id in parent_ids:
                self.add_child(father_id, task_id)
        
                #This ids have been modified, hence, they need to be synchronized. 
            self.__to_synchronize.append(task_id)
            self.__to_synchronize.extend(parent_ids)
            self.synchronize()    
                
    def get_normal_children_ids(self, task_id):
        '''
        Returns only the ids of the children whose type is normal
        '''
        ids_to_return = list()
        if task_id in self.__id_task_dict:
            children_ids = self.__id_task_dict[task_id].getChildren()
            
            for c_id in children_ids:
                if c_id in self.__id_task_dict:
                    if self.__id_task_dict[c_id].getType()=="normal":
                        ids_to_return.append(c_id)
            
        return ids_to_return
        
        
    def get_children_ids(self, task_id):
        '''
        Returns the children ids of the task.  If the task does not exist, it returns an empty list
        '''
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].getChildren()
        return []
    
    def get_parents(self, task_id):
        '''
        Returns the parent ids of the task. If the task does not exist, it returns an empty list
        '''
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].getParents()
        return []
    
    def get_parents_output(self, task_id):
        
        to_return = list()
        if task_id in self.__id_task_dict:
            task = self.__id_task_dict[task_id]
            parent_ids = task.getParents()
            for p_id in parent_ids:
                to_return.append(self.__id_task_dict[p_id].getOutput())
                
        return to_return
    
    def get_input_args(self, task_id):
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].getInput_args()
        return None
    
    def get_callable_object(self, task_id):
        '''
        Returns the callable object that corresponds to that task id
        Returns None if task_id not in dictionary
        '''
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].getCallable_object()
        return None
    
    def set_callable_object(self, task_id, callable_object):
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setCallable_object(callable_object)
            
            self.__to_synchronize.append(task_id)
            self.synchronize()
    
    def are_parents_done(self, task_id):
        '''
        If the parents of the task are done, or if the task does not exist, it returns True.
        Otherwise, it returns False.
        '''
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].are_parents_done()
        return True
    
    def get_state(self, task_id):
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].getState()
        return None
    
    def is_running(self, task_id):
        '''
        Returns true of the task is running (or already sent to the queue)
        '''
        if task_id in self.__id_task_dict:
            state=self.__id_task_dict[task_id].getState()
            return state=="running"
        
        return False
    
    def set_state_running(self, task_id):
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setStateRunning()
            self.__to_synchronize.append(task_id)
            self.synchronize()
            
    def set_state_failed(self, task_id):
        '''
        Changes the state of task represented by task id to failed
        '''
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setStateFailed()
        
            self.__to_synchronize(task_id)
            self.synchronize()
            
    def set_done(self, task_id):
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setDone()
        
            self.__to_synchronize.append(task_id)
            self.synchronize()
    
    def set_output(self, task_id, t_output):
        '''
        Sets the output of task_id
        '''
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setOutput(t_output)
            
            self.__to_synchronize.append(task_id)
            self.synchronize()

    def set_father_finished(self, child_id, father_id):
        if child_id in self.__id_task_dict:
            self.__id_task_dict[child_id].father_finished(father_id)
        
            self.__to_synchronize.append(child_id)
            self.synchronize()
        
    def add_child(self, task_id, child_id):
        if task_id in self.__id_task_dict and child_id in self.__id_task_dict:
            task = self.__id_task_dict[task_id]
            child = self.__id_task_dict[child_id]
            task.addChild(child_id)
            if task.isDone():
                child.father_finished(task_id)
            if task.getState()=='failed':
                child.setCallable_object(ExceptionTask(child.getId(), child.getInput_args()))
        
            self.__to_synchronize.append(task_id)
            self.__to_synchronize.append(child_id)
            self.synchronize()
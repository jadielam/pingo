'''
Created on Oct 30, 2014

@author: jadiel
'''

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
    
    def __init__(self):
        self.__id_task_dict=dict()
    
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
        else:
            if not isinstance(parent_ids, list):
                temp_parents=list()
                temp_parents.append(parent_ids)
                parent_ids=temp_parents
             
            task=Task(task_id, parent_ids, input_args, callable_object, ttype)
            
            self.__id_task_dict[task_id]=task
            
            for father_id in parent_ids:
                self.add_child(father_id, task_id)
            
        
        
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
    
    def are_parents_done(self, task_id):
        '''
        If the parents of the task are done, or if the task does not exist, it returns True.
        Otherwise, it returns False.
        '''
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].are_parents_done()
        return True
    
    def set_state_failed(self, task_id):
        '''
        Changes the state of task represented by task id to failed
        '''
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setStateFailed()
            
    def set_done(self, task_id):
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setDone()
    
    def set_output(self, task_id, t_output):
        '''
        Sets the output of task_id
        '''
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setOutput(t_output)

    def set_father_finished(self, child_id, father_id):
        if child_id in self.__id_task_dict:
            self.__id_task_dict[child_id].father_finished(father_id)
        
    def add_child(self, task_id, child_id):
        if task_id in self.__id_task_dict and child_id in self.__id_task_dict:
            task = self.__id_task_dict[task_id]
            child = self.__id_task_dict[child_id]
            task.addChild(child_id)
            if task.isDone():
                child.father_finished(self.id)
            if task.getState()=='failed':
                child.setCallable_object(ExceptionTask(child.getId(), child.getInput_args()))
'''
Created on Oct 30, 2014

@author: jadiel
'''
import pickle

from tasksgraph.systemtasks import ExceptionTask

class Task:
    def __init__(self, task_id, parents, input_args, task_class, ttype='normal', reporters_ids=[], reportees_ids=[]):
        self.id=task_id
        if parents == None:
            self.parents=list()
        else:
            self.parents=parents
        
        self.task_class=task_class  #class of the callable object
        self.input_args=input_args
        self.children=list()
        self.output=None
        self.done=False
        self.state="created"
        self.ids_of_parents_done=set()
        self.__type=ttype
        self.__output_file=None
        
        #The concept of reporters and reporters is a task that I don't depend on (I can start running myself
        #even if the reporter has not finished. But if the reporter has finished when I start running, I
        #get to see the output of the reporter.
        self.reporters_ids=set(reporters_ids)    #
        
        #A reportee is the inverse concept to that of a reporter. A reportee is a task to which I report.
        self.reportees_ids=set(reportees_ids)
        
        #Reportee and reporter are simply grammatical distinctions. Because unless the core engine and the task
        #task graph take care of executing the idea, nothing will happen. Is something simply optional.
        
    def getReporters_ids(self):
        return self.reporters_ids
    
    def getReportees_ids(self):
        return self.reportees_ids
    
    def addReporter_id(self, reporter_id):
        self.reporters_ids.add(reporter_id)
        
    def addReportee_id(self, reportee_id):
        self.reportees_ids.add(reportee_id)
    
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
        self.done=True
    
    def setStateFailed(self):
        self.state="failed"
    
    def getState(self):
        return self.state
            
    def isDone(self):
        return self.done
    
    def setDone(self):
        self.state="finished"
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
    
    def setTask_class(self, task_class, input_args):
        self.task_class=task_class
        self.input_args=input_args
    
    def getTask_class(self):
        return self.task_class
    
    def getCallable_object(self):
        return self.task_class(self.id, self.input_args)
    
    def setOutput(self, output):
        self.output=output
        
    def getOutput(self):
        return self.output           
    
    def __getstate__(self):
        '''
        Used to avoid pickle issues when storing the callable object.
        '''
        state = self.__dict__.copy()
        return state
    
    def __setstate__(self, d):
        '''
        Used to avoid pickle issues when storing the callable object.
        '''
        self.__dict__ = d
            
class TaskGraph:
    
    def __init__(self, synchronization_file=None):
        self.__id_task_dict=dict()
        self.__to_synchronize=list()
        self.__synchronization_file = synchronization_file
        self.__f = None
        
        #This piece of code first attempts to open a file in read mode.
        #If the file does not exist, it simply creates a new file and opens it in write mode.
        if synchronization_file != None:
            try:
                self.__f=open(self.__synchronization_file, "rb+")
                self.__read_synchronization_file()
            except:
                try:
                    self.__f=open(self.__synchronization_file, "wb+")
                except:
                    print("Error opening synchronization file at TaskGraph.__init__")
                
    def __del__(self):
        try:
            self.__f.close()
        except:
            pass
    
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
            
            while True:
                task = pickle.load(self.__f)
                
                
                self.__id_task_dict[task.getId()] = task
                
        
        except EOFError:
            #Close the file that was opened.
            #self.__f.truncate(0)
            pass
                        
        except FileNotFoundError:
            print("File "+str(self.__synchronization_file)+" does not exist")
        except:
            print("Some other error")
        
    def __append_synchronize(self, task_id):
        if task_id in self.__id_task_dict:
            state = self.__id_task_dict[task_id].getState()
            
            if state == "failed" or state =="finished":
                self.__to_synchronize.append(task_id)
    
    def __extend_synchronize(self, ids):
        filtered_ids = [a for a in ids if self.finished(a)]
        self.__to_synchronize.extend(filtered_ids)
        
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
        
        if ((len(self.__to_synchronize)>4) or force) and self.__f!=None:
            synchronizing=self.__to_synchronize.copy()
            self.__to_synchronize.clear()
            
            #Note, that we only synchronize the tasks that are already finished.
                       
            tasks = [self.__id_task_dict[task_id] for task_id in synchronizing if (self.finished(task_id) or self.failed(task_id))]
            try:
                                    
                for i in range(len(tasks)):
                    
                    task=tasks[i]
                    pickle.dump(task, self.__f)
                    
             
            except:
                print("Error in TaskGraph.synchronize()")
                #Code left here for future reference, if I ever implement this that way.
                # We take care of things this way anticipating some multi-hreading in this method.
                
                #synchronizing.extend(self.__to_synchronize)
                #self.__to_synchronize = synchronizing
            
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
    
    def failed(self, task_id):
        if not task_id in self.__id_task_dict:
            return False
        return self.__id_task_dict[task_id].getState()=="failed"
    
    def children_finished(self, task_id):
        '''
        Returns True if every child of task_id finished, otherwise it returns false
        '''
        if task_id in self.__id_task_dict:
            task=self.__id_task_dict[task_id]
            children_ids=task.getChildren()
            to_return=True
            for child_id in children_ids:
                if not self.finished(child_id):
                    to_return=False
            return to_return
        else:
            return False
            
    def create_task(self, task_id, parent_ids, input_args, callable_object, ttype="normal", reporters_ids=[], reportees_ids=[]):
        if parent_ids==None:
            task=Task(task_id, None, input_args, callable_object, ttype, reporters_ids, reportees_ids)
            self.__id_task_dict[task_id]=task
            self.__append_synchronize(task_id)
            
        else:
            if not isinstance(parent_ids, list):
                temp_parents=list()
                temp_parents.append(parent_ids)
                parent_ids=temp_parents
             
            task=Task(task_id, parent_ids, input_args, callable_object, ttype, reporters_ids, reportees_ids)
            
            self.__id_task_dict[task_id]=task
            
            for father_id in parent_ids:
                self.add_child(father_id, task_id)
        
                #This ids have been modified, hence, they need to be synchronized. 
            self.__append_synchronize(task_id)
            self.__extend_synchronize(parent_ids)
                    
        for reporter_id in reporters_ids:
            if reporter_id in self.__id_task_dict:
                self.__id_task_dict[reporter_id].addReportee_id(task_id)
                self.__append_synchronize(reporter_id)
        
        for reportee_id in reportees_ids:
            if reportee_id in self.__id_task_dict:
                self.__id_task_dict[reportee_id].addReporter_id(task_id)
                self.__append_synchronize(reportee_id)
        
        self.synchronize()            
                
    def get_normal_children_ids(self, task_id):
        '''
        Returns only the ids of the children whose type is normal
        Normal type is the type assigned to tasks that were placed in the queue.
        Any other task that is run but is not placed in the queue must have some other type.
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
    
    def get_reporters_output(self, task_id):
        #returns a list of outouts of the repoorters to task_id if it is that the reporters have finished.
        #Otherwise it returns None in that position of the list.
        to_return = list()
        if task_id in self.__id_task_dict:
            task=self.__id_task_dict[task_id]
            
            reporters_ids=task.getReporters_ids()
            for reporter_id in reporters_ids:
                if reporter_id in self.__id_task_dict:
                    to_return.append(self.__id_task_dict[reporter_id].getOutput())
                    
        return to_return
            
    def get_input_args(self, task_id):
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].getInput_args()
        return None
    
    def get_task_class(self, task_id):
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].getTask_class()
        return None
    
    def get_callable_object(self, task_id):
        '''
        Returns the callable object that corresponds to that task id
        Returns None if task_id not in dictionary
        '''
        if task_id in self.__id_task_dict:
            return self.__id_task_dict[task_id].getCallable_object()
        return None
    
    def set_task_class(self, task_id, task_class, input_args=None):
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setTask_class(task_class, input_args) 
            
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
            
    def set_state_failed(self, task_id):
        '''
        Changes the state of task represented by task id to failed
        '''
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setStateFailed()
        
            self.__append_synchronize(task_id)
            self.synchronize()
            
    def set_done(self, task_id):
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setDone()
        
            self.__append_synchronize(task_id)
            self.synchronize()
    
    def set_output(self, task_id, t_output):
        '''
        Sets the output of task_id
        '''
        if task_id in self.__id_task_dict:
            self.__id_task_dict[task_id].setOutput(t_output)
            
            self.__append_synchronize(task_id)
            self.synchronize()

    def set_father_finished(self, child_id, father_id):
        if child_id in self.__id_task_dict:
            self.__id_task_dict[child_id].father_finished(father_id)
        
            self.__append_synchronize(child_id)
            self.synchronize()
        
    def add_child(self, task_id, child_id):
        if task_id in self.__id_task_dict and child_id in self.__id_task_dict:
            task = self.__id_task_dict[task_id]
            child = self.__id_task_dict[child_id]
            task.addChild(child_id)
            if task.isDone():
                child.father_finished(task_id)
            if task.getState()=='failed':
                child.setTask_class(ExceptionTask, child.getInput_args())
        
            self.__append_synchronize(task_id)
            self.__append_synchronize(child_id)
            self.synchronize()
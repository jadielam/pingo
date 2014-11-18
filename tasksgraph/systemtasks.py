'''
Created on Nov 3, 2014

@author: jadiel
'''
from tasksgraph.callableobject import AbstractTask
from tasksgraph.exceptions import ConcatenatingException

class TaskMapSynchronizer(AbstractTask):
    '''
    The purpose of this class is to write to disk the new task nodes that have been
    added or modified in the task graph.  
    The task receives as input a map of key-value pairs. 
    'list_of_tasks' is the list of tasks to be appended
    'output_file' is the file path of the file to which they will be outputted.
    
    It then appends the tasks nodes into the file.
    TODO: Currently not in use.
    '''
    def __call__(self):
        import pickle
        
        tasks=self.input_args['list_of_tasks']
        output_file_path=self.input_args['output_file']
        
        f=open(output_file_path, "w+")
        for task in tasks:
            pickle.dump(task, f)
        f.close()
        
        return True

class OutputWriterTask(AbstractTask):
    
    """
    The purpose of this class is to write to a file the output of its parent.
    It takes as input a map that contains the task_id and other keys that
    will be added later.
    """
    def __call__(self):
        import pickle
        from tasksgraph import utilities
                
        output_task_id=self.input_args
        parents_output=self.parents_output
        
        if len(parents_output)>1:
            raise Exception("More than one parent in OutputWriterTask")
        
        file_name=utilities.create_output_file_name(output_task_id)
        
        for p_output in parents_output:
            with open(file_name, "wb") as output:
                pickle.dump((output_task_id, p_output), output, pickle.HIGHEST_PROTOCOL)
        
        #Returns the task id of the task whose output was saved into file_name
        return (output_task_id, file_name)
        
class ExceptionTask(AbstractTask):
    def __call__(self):
        new_message="Error in task "+str(self.task_id)+" because of parent malfunction."
        raise ConcatenatingException(self.parents_output, new_message)

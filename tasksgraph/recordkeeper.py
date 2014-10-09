'''
Created on Oct 6, 2014

@author: jadiel
'''

from tasksgraph import TaskGraph

def write_function(output_task_args, parents_output, task_id):
    import time
    import pickle
    '''
    This function writes the output to file, together with some extra metadata
    '''
    file_name=output_task_args['file_name']
    output_task_id=output_task_args['task_id']   #Note that task_id is the id of the output task, and output_task_id is the id of the task that produced the output
    parent_ids=output_task_args['task_id']
    current_date=time.strftime("%d/%m/%Y %H:%M:%S")
    metadata=(output_task_id, parent_ids, current_date)
        
    print("Writing to file "+file_name)
    if len(parents_output)>1:
        raise Exception("More than one parent")
    
    for p_output in parents_output:
        with open(file_name, 'wb') as output:
            pickle.dump((metadata, p_output), output, pickle.HIGHEST_PROTOCOL)
    
def read_function(input_task_args, parents_output, task_id):
    import pickle
    file_name=input_task_args['file_name']
    
    with open(file_name, 'rb') as f:
        content=pickle.load(f)
    
    #content[0] is metadata
    #content[1] is the output from the parent.
    return content[1]

def read_keeper_file(keeper_path):
    '''
    Reads the key-value pairs of the keeper file
    key: task id
    value: task output file
    '''
    toReturn=dict()
    #If there is something wrong with the file, just return an empty dictionary.
    try:
        with open(keeper_path, "r") as f:
            for line in f:
                line=line.strip()
                a=line.find(",")
                task_id=int(line[:a])
                task_output_path=line[a+1:]
                toReturn[task_id]=task_output_path
    except:
        return toReturn
    
    return toReturn
    
class RecordKeeper:
    
    def __init__(self, pool_size, keeper_path):
        self.taskgraph=TaskGraph(pool_size)
        self.keeper_path=keeper_path
        self.task_taskoutputfile_dict=read_keeper_file(keeper_path)
    
    def __build_task_output_path(self, task_id):
        return str(self.keeper_path)+"_"+str(task_id)
    
    def create_task(self, parent_ids, input_value, user_function, context=None):
        
        next_task_id=self.taskgraph.pick_next_task_id(context)
        if next_task_id in self.task_taskoutputfile_dict:
            input_task_args=dict()
            input_task_args['file_name']=self.__build_task_output_path(next_task_id)
            task_id=self.taskgraph.create_task(parent_ids, input_task_args, read_function)
        else:
            task_id=self.taskgraph.create_task(parent_ids, input_value, user_function, context)
            output_task_args=dict()
            output_task_args['file_name']=self.__build_task_output_path(task_id)
            output_task_args['task_id']=task_id
            output_task_args['parent_ids']=parent_ids
            self.taskgraph.create_task([task_id], output_task_args, write_function)
        
        return task_id
    
    def join(self):
        return self.taskgraph.join()
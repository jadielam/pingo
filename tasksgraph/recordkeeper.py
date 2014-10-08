'''
Created on Oct 6, 2014

@author: jadiel
'''

from tasksgraph import TaskGraph
import pickle

def write_function(output_task_args, parents_output, task_id):
    '''
    This function writes the output to file
    '''
    file_name=output_task_args['file_name']
    output_task_id=output_task_args['task_id']   #Note that task_id is the id of the output task, and output_task_id is the id of the task that produced the output

    print("Writing to file "+file_name)
    if len(parents_output)>1:
        raise "More than one parent"
    
    for p_output in parents_output:
        with open(file_name, 'w+') as output:
            pickle.dump(p_output, output, pickle.HIGHEST_PROTOCOL)
    
def read_function(input_task_args, parents_output, task_id):
    
    file_name=input_task_args['file_name']
    
    with open(file_name, 'rb') as input:
        toReturn=pickle.load(input)
    
    return toReturn

def read_keeper_file(keeper_path):
    '''
    Reads the key-value pairs of the keeper file
    key: task id
    value: task output file
    '''
    toReturn=dict()
    with open(keeper_path, "r") as input:
        for line in input:
            line=line.strip()
            a=line.find(",")
            task_id=int(line[:a])
            task_output_path=line[a+1:]
            toReturn[task_id]=task_output_path
            
    return toReturn
    
class RecordKeeper:
    
    def __init__(self, pool_size, keeper_path):
        self.taskgraph=TaskGraph(pool_size)
        self.keeper_path=keeper_path
        self.task_taskoutputfile_dict=read_keeper_file(keeper_path)
        
    def create_task(self, parent_ids, input_value, user_function):
        task_id=self.taskgraph.create_task(parent_ids, input_value, user_function)
        
        output_task_args=dict()
        output_task_args['file_name']=self.keeper_path+"_"+str(task_id)
        output_task_args['task_id']=task_id
        self.taskgraph.create_task([task_id], output_task_args, write_function)
        
        return task_id
        
        
        
        
        
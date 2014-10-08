'''
Created on Oct 6, 2014

@author: jadiel
'''

import tasksgraph
import multiprocessing, logging

def sum(input_args, parents_output, task_id):
    a=0
    for i in range(input_args):
        for j in range(input_args):
            for k in range(input_args):
                
                a+=k
    return a

def join_results(input_args, parents_output, task_id):
    
    5/0
    toReturn="\n".join([str(a) for a in parents_output])
    
    return toReturn

def print_result(input_args, parents_output, task_id):
    
    print(parents_output)
    
def main():
    taskGraph=tasksgraph.TaskGraph(4)
    
    logger=multiprocessing.log_to_stderr()
    logger.setLevel(logging.INFO)
    
    parent_ids=list()
    for i in range(10):
        task_id=taskGraph.create_task(None, 30+i, sum)
        parent_ids.append(task_id)
        
    joiner_task=taskGraph.create_task(parent_ids, None, join_results)
    joiner_task=taskGraph.create_task([joiner_task], None, join_results)
    joiner_task=taskGraph.create_task([joiner_task], None, join_results)
    printer_task=taskGraph.create_task([joiner_task], None, print_result)
    
    
    taskGraph.join()
    
    
    
    
    
if __name__=="__main__":
    main()      
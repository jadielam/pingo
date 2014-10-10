'''
Created on Oct 6, 2014

@author: jadiel
'''

import recordkeeper
import multiprocessing, logging
import sys

def sum(input_args, parents_output, task_id):
    a=0
    for i in range(input_args):
        for j in range(input_args):
            for k in range(input_args):
                
                a+=k
    return a

def join_results(input_args, parents_output, task_id):
    
    toReturn="\n".join([str(a) for a in parents_output])
    return toReturn

def print_result(input_args, parents_output, task_id):
    
    print(parents_output)
    
def main():
    iterations=int(sys.argv[1])
    taskGraph=recordkeeper.RecordKeeper(4, "testing")
    
    logger=multiprocessing.log_to_stderr()
    logger.setLevel(logging.INFO)
    
    parent_ids=list()
    for i in range(iterations):
        task_id=taskGraph.create_task(None, 30, sum)
        parent_ids.append(task_id)
        
    joiner_task=taskGraph.create_task(parent_ids, None, join_results)
    joiner_task1=taskGraph.create_task([joiner_task], None, join_results)
    joiner_task2=taskGraph.create_task([joiner_task1], None, join_results)
    printer_task=taskGraph.create_task([joiner_task2], None, print_result)
    
    taskGraph.join()
    
    
    
    
    
if __name__=="__main__":
    main()      
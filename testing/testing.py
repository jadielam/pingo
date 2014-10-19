
'''
Created on Oct 15, 2014

@author: jadiel
'''

import multiprocessing
from test1 import Task1

def task_finished(outp):
    print("in task finished")
    print(outp)
    
def main():
    
    manager=multiprocessing.Manager()
    queue=manager.Queue()
    
    pool=multiprocessing.Pool(4)
    task1=Task1()
    parent_conn, child_conn=multiprocessing.Pipe()
    condition=manager.Condition()
    task1.queue=queue
    task1.pipe_conn=child_conn  
    task1.condition=condition
    result=pool.apply_async(func=task1,
                            callback=task_finished)
    
    
    

if __name__=="__main__":
    main()
    
    
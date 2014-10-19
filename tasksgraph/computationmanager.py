'''
Created on Oct 15, 2014

@author: jadiel
'''

from tasksgraph import coreengine
from tasksgraph import context
import multiprocessing
import threading
import collections
from multiprocessing import Process
from tasksgraph.coreengine import CoreEngine


            
def create_pool_of_pipes_conditions(pipes, conditions):
    pipes_conditions=list()
    for i in range(len(pipes)):
        pipes_conditions.append((pipes[i], conditions[i]))
    to_return=collections.deque(pipes_conditions)
    return to_return

class ComputationManager(object):
    
    def __init__(self, pool_size, first_task_class, task_input):
        self.__pool_size=pool_size
        self.__first_task=first_task_class("root", task_input)
            
    def __call__(self):
        #1. Create a manager
        manager=multiprocessing.Manager()
        
        #2. Create a queue from the manager
        queue=manager.Queue()
        
        #3. Assign the first job from the user to the queue
        queue.put((self.__first_task, None))

        #6. Create the Core Engine thread and send it running.
        core_engine_thread=threading.Thread(target=CoreEngine(self.__pool_size, queue, manager, []))
        core_engine_thread.start()
        
        #7. call join on the queue.
        queue.join()
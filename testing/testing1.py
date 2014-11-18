'''
Created on Oct 17, 2014

@author: jadiel
'''
from tasksgraph.callableobject import AbstractTask
from tasksgraph.computationmanager import ComputationManager
import multiprocessing

class SumTask(AbstractTask):
    
    def __call__(self):
        print("In sumTask")
        suma=0
        limit=self.input_args
        
        for i in range(limit):
            for j in range(limit):
                for k in range(limit):
                    suma+=(i+j+k)
        
        self.output=suma
        print("In SumTask")
        return suma

class JoinTask(AbstractTask):
    
    def __call__(self):
        
        print("In JoinTask")
        to_return="\n".join([str(a) for a in self.parents_output])
        self.output=to_return
        return to_return

class PrintTask(AbstractTask):
    
    def __call__(self):
        
        print("In PrintTask")
        print(self.parents_output)
    

class MainTask(AbstractTask):
    
    def __call__(self):
        print("In MainTask")
        sum_ids=[]
        for i in range(5):
            task_id=self.create_task(SumTask, [self.task_id], 50)
            sum_ids.append(task_id)
        
        join_task=self.create_task(JoinTask, sum_ids, None)
        print_task=self.create_task(PrintTask, [join_task], None)
        print("At the end of Main Task")
        

def main():
    
    
    #2. Create the Computation Manager and pass it the class to run.
    c_manager=ComputationManager(4, "testing_jadiel", MainTask, None)
    
    #3. Run the Manager
    c_manager_process=multiprocessing.Process(target=c_manager)
    c_manager_process.start()
    c_manager_process.join()
    
        

if __name__=="__main__":
    main()
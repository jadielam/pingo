'''
Created on Oct 15, 2014

@author: jadiel
'''

from abstractclass import AbstractTask

class Test1(AbstractTask):
    def __call__(self, **args):
        print("In Test1: to_run: "+str(input))
        return 1

class Test2(AbstractTask):
    def __call__(self, **args):
        print("In Test2: to_run: "+str(input))
        return 1
    
class Task1(AbstractTask):
    
    def __call__(self):
        print("In Test2: to_run: "+str(input))
        self.create_task(Test1)
        return 1
    
        
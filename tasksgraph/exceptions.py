'''

Created on Oct 8, 2014

@author: jadiel

Contains the definition of all the exceptions that will be used in the tasksgraph package
'''

class ConcatenatingException(Exception):
    def __init__(self, parent_exception, new_data):
        self.value=(str(parent_exception), str(new_data))
    
    def __str__(self):
        return repr(self.value)
'''
Created on Nov 30, 2014

@author: jadiel
'''

def chunks(li, n):
    '''
    Returns a list of n lists that constitute a partition of size n of list li
    '''
    
    if n < 1:
        n = 1
        
    remainder = len(li) % n
    exact_length = len(li) + n * remainder
    partition_size = exact_length // n
    
    return [li[i:i + partition_size] for i in range(0, len(li), partition_size)]
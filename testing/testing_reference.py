'''
Created on Nov 25, 2014

@author: jadiel
'''

def list_reference(lista):
    lista[0]=2
    return lista


def main():
    lista1 = [3, 3, 3, 3, 3]
    lista2 = list_reference(lista1)
    print(lista1)
    print(lista2)
    
if __name__ == "__main__":
    main()
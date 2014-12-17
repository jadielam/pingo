'''
Created on Dec 15, 2014

@author: jadiel
'''

from tasksgraph.callableobject import AbstractTask
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import f1_score

class Corpus:
    '''
    Contains the data together with the modifications it has suffered.
    '''
    def __init__(self, training_x, training_y, testing_x, testing_y, modifications = None):
        self.training_x = training_x
        self.training_y = training_y
        self.testing_x = testing_x
        self.testing_y = testing_y
        if modifications == None:
            self.modifications = list()
        else: self.modifications = modifications
    
    def add_modification(self, modification):
        self.modifications.append(modification)        

class Results:
    '''
    Contains the history of the pipeline that produced these results, together with the results
    '''
    def __init__(self, training_file, testing_file, pipeline_history, results):
        self.training_file = training_file
        self.testing_file = testing_file
        self.pipeline_history = pipeline_history
        self.results = results
    
    def __str__(self):
        
        to_print = list()
        to_print.append("Result: ")
        to_print.append("(")
        for a in self.pipeline_history:
            to_print.append(str(a))
            to_print.append(", ")
        to_print.append(")")
        
        to_print.append(",\t")
        to_print.append(str(self.results))
        
        return "".join(to_print)
    
    def __lt__(self, other):
        return self.results['fmeasure'] < other.results['fmeasure']    
    

class PreprocessorTask(AbstractTask):
    '''
    Runs preprocessing on the data and returns a new corpus with the preprocessed data.
    Receives as input a map that contains keys: corpus and preprocessor
    '''
    def __call__(self):
        corpus = self.input_args['corpus']
        preprocessor = self.input_args['preprocessor']
        
        preprocessor.fit(corpus.training_x, corpus.training_y)
        new_training_x = preprocessor.transform(corpus.training_x)
        new_testing_x = preprocessor.transform(corpus.testing_x)
        modifications = corpus.modifications.copy()
        modifications.append(preprocessor.__class__.__name__)
        
        return Corpus(new_training_x, corpus.training_y, new_testing_x, corpus.testing_y, modifications)
    
class FilterTask(AbstractTask):
    '''
    Very similar to the Preprocessor task, but instead of receiving the corpus from the input_args
    it receives it from the output of its parent task
    '''
    def __call__(self):
        corpus = self.parents_output[0]
        preprocessor = self.input_args['filter']
        preprocessor.fit(corpus.training_x, corpus.training_y)
        new_training_x = preprocessor.transform(corpus.training_x)
        new_testing_x = preprocessor.transform(corpus.testing_x)
        modifications = corpus.modifications.copy()
        modifications.append(preprocessor.__class__.__name__)
            
        return Corpus(new_training_x, corpus.training_y, new_testing_x, corpus.testing_y, modifications)



class ModelTask(AbstractTask):
    '''
    Trains and test a given corpus
    '''
    def __call__(self):
        
        corpus = self.parents_output[0]
        model = self.input_args['model']
        model.fit(corpus.training_x, corpus.training_y)
        y_predict = model.predict(corpus.testing_x)
        y_real = corpus.testing_y
       
        results_d = dict()
        results_d['accuracy'] = accuracy_score(y_real, y_predict)
        results_d['fmeasure'] = f1_score(y_real, y_predict)
        results_d['precision'] = precision_score(y_real, y_predict)
        results_d['recall'] = recall_score(y_real, y_predict)
        
        
        modifications = corpus.modifications.copy()
        modifications.append(model.__class__.__name__)
        
        results = Results(self.input_args['training_file'], self.input_args['testing_file'], modifications, results_d)
        
        print(results)
        
        return results
          

class ResultsTask(AbstractTask):
    '''
    It takes the results and prints sorts them and prints them.
    '''
    def __call__(self):
        '''
        The idea is to sort the results and pick the best one.
        '''
        results = self.parents_output
        results.sort()
        
        for result in results:
            print(result)
        

from sklearn.datasets import load_svmlight_file

class Pipeline(AbstractTask):
    
    def __call__(self):
        
        #1. Read the training and testing file
        training_file_path = self.input_args['training_file']
        testing_file_path = self.input_args['testing_file']
        n_feat = self.input_args['n_features']
        X_train, y_train = load_svmlight_file(training_file_path, n_features = n_feat)
        X_test, y_test = load_svmlight_file(testing_file_path, n_features = n_feat)
        X_train = X_train.toarray()
        X_test = X_test.toarray()
        
        corpus = Corpus(X_train, y_train, X_test, y_test)
        
        #2. For each preprocessor, pre-process the data and return the pre-processed corpus.
        preprocessors = self.input_args['preprocessors']
        preprocessors_ids = list()
        for preprocessor in preprocessors:
            p_input = dict()
            p_input['corpus'] = corpus
            p_input['preprocessor'] = preprocessor
            preprocessors_ids.append(self.create_task(PreprocessorTask, [], p_input))
        
        #3. For each filter, for each result produced by the preprocessors, filter the data
        filters = self.input_args['filters']
        filters_ids = list()
        for filter in filters:
            f_input = dict()
            f_input['filter'] = filter
            for preprocessor_id in preprocessors_ids:
                filters_ids.append(self.create_task(FilterTask, [preprocessor_id], f_input))
    
        #4. For each result produced by filters train a model and then test it and output results
        models = self.input_args['models']
        models_ids = list()
        for model in models:
            m_input = dict()
            m_input['model'] = model
            m_input['training_file'] = training_file_path
            m_input['testing_file'] = testing_file_path
            for filter_id in filters_ids:
                models_ids.append(self.create_task(ModelTask, [filter_id], m_input))
        
        #5. Sort all reduce produced by top to bottom.
        r_input = dict()
        r_input['training_file'] = training_file_path
        r_input['testing_file'] = testing_file_path
        self.create_task(ResultsTask, models_ids, None)
    

import multiprocessing
import threading
from tasksgraph.coreengine import CoreEngine

class PipelineManager:
    def __init__(self, preprocessors, filters, models, training_file, testing_file, n_features, nproc):
        self.input_values=dict()
        self.input_values['preprocessors'] = preprocessors
        self.input_values['filters'] = filters
        self.input_values['models'] = models
        self.input_values['training_file'] = training_file
        self.input_values['testing_file'] = testing_file
        self.input_values['n_features'] = n_features
        self.nproc = nproc
        
    def __call__(self):
        
        manager = multiprocessing.Manager()
        
        queue = manager.Queue()
        
        queue.put((Pipeline, 'root', self.input_values, None))
        
        core_engine_thread = threading.Thread(target = CoreEngine(self.nproc, queue, manager, [], None))
       
        core_engine_thread.start()
        
        queue.join()
        
        
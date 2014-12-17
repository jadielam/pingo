'''
Created on Dec 16, 2014

@author: jadiel

'''

from ml_pipeline.pipeline import PipelineManager
import multiprocessing
import sys
import sklearn
import sklearn.preprocessing
from sklearn.pipeline import Pipeline
import sklearn.svm
import sklearn.linear_model
import sklearn.decomposition
import sklearn.feature_selection
import sklearn.naive_bayes
import sklearn.tree
import sklearn.neighbors


def create_preprocessors():
    preprocessors = list()
    
    standard_scaler = sklearn.preprocessing.StandardScaler()
    minMax_scaler = sklearn.preprocessing.MinMaxScaler()
    minMax_scaler1 = sklearn.preprocessing.MinMaxScaler(feature_range = (-1, 1))
    normalizer1 = sklearn.preprocessing.Normalizer(norm = 'l1')
    normalizer2 = sklearn.preprocessing.Normalizer(norm = 'l2')
    binarizer = sklearn.preprocessing.Binarizer(threshold = 0.01)
    
    preprocessors.append(standard_scaler)
    preprocessors.append(minMax_scaler)
    preprocessors.append(minMax_scaler1)
    preprocessors.append(normalizer1)
    preprocessors.append(normalizer2)
    #preprocessors.append(binarizer)
      
    return preprocessors

def create_filters():
    filters = list()
    
    identity = sklearn.feature_selection.SelectPercentile(score_func = sklearn.feature_selection.chi2, percentile = 100)
    chi2 = sklearn.feature_selection.SelectPercentile(score_func = sklearn.feature_selection.chi2, percentile = 10)
    f_classif = sklearn.feature_selection.SelectPercentile(score_func = sklearn.feature_selection.f_classif, percentile = 10)
    pca = sklearn.decomposition.PCA(n_components = 'mle')
    sparse_pca = sklearn.decomposition.SparsePCA()
    fast_ica = sklearn.decomposition.FastICA()
    
    filters.append(identity)
    filters.append(chi2)
    filters.append(f_classif)
    #filters.append(pca)
    #filters.append(sparse_pca)
    #filters.append(fast_ica)
    
    return filters

def create_models():
    models = list()
    
    logistic_regression_l1 = sklearn.linear_model.LogisticRegression(penalty = 'l1')
    logistic_regression_l2 = sklearn.linear_model.LogisticRegression(penalty = 'l2')
    sgd_lr = sklearn.linear_model.SGDClassifier(loss = 'log')
    sgd_svm = sklearn.linear_model.SGDClassifier(loss = 'hinge')
    sgd_elastic_net = sklearn.linear_model.SGDClassifier(loss = 'log', penalty = 'elasticnet')
    perceptron = sklearn.linear_model.Perceptron()
    passive_aggresive_classifier = sklearn.linear_model.PassiveAggressiveClassifier()
    #polynomial_d2 = Pipeline([('poly', sklearn.preprocessing.PolynomialFeatures(degree = 2)), ('linear', sklearn.linear_model.LogisticRegression(penalty = 'l1'))])
    #polynomial_d3 = Pipeline([('poly', sklearn.preprocessing.PolynomialFeatures(degree = 3)), ('linear', sklearn.linear_model.LogisticRegression(penalty = 'l1'))])
    svc = sklearn.svm.SVC()
    nuSVC = sklearn.svm.NuSVC()
    linearSVC = sklearn.svm.LinearSVC()
    kneighbors = sklearn.neighbors.KNeighborsClassifier()
    gaussian_nb = sklearn.naive_bayes.GaussianNB()
    multinomial_nb = sklearn.naive_bayes.MultinomialNB()
    bernoulli_nb = sklearn.naive_bayes.BernoulliNB()
    decision_tree = sklearn.tree.DecisionTreeClassifier()
    
    models.append(logistic_regression_l1)
    models.append(logistic_regression_l2)
    models.append(sgd_lr)
    models.append(sgd_svm)
    models.append(sgd_elastic_net)
    models.append(perceptron)
    models.append(passive_aggresive_classifier)
    #models.append(polynomial_d2)
    #models.append(polynomial_d3)
    models.append(svc)
    models.append(nuSVC)
    models.append(linearSVC)
    models.append(kneighbors)
    #models.append(gaussian_nb)
    #models.append(multinomial_nb)
    #models.append(bernoulli_nb)
    #models.append(decision_tree)
      
    return models

def main():
    
    preprocessors = create_preprocessors()
    filters = create_filters()
    models = create_models()
    training_file = sys.argv[1]
    testing_file = sys.argv[2]
    n_features = int(sys.argv[3])
    nproc = 4
    ml_manager = PipelineManager(preprocessors, filters, models, training_file, testing_file, n_features, nproc)
    ml_manager_process = multiprocessing.Process(target = ml_manager)
    ml_manager_process.start()
    ml_manager_process.join()

if __name__=="__main__":
    main()
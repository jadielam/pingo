'''
Created on Dec 4, 2014

@author: jadiel
'''

from __future__ import division
from collections import namedtuple
from collections import deque
import math
import sys

Observation = namedtuple('Observation', ['vector', 'klass'], verbose = False)

decision_tree_root = None

class FeatureNode:
    def __init__(self, feature_number):
        
        #By doing this I am keeping the list of features
        self.feature_number = feature_number
            
        self.child_nodes = list()
    
    def add_parent(self, parent_node):
        self.parent_node = parent_node
        if self.parent_node != None:
            if self.parent_node.parent_node != None:
                self.features = self.parent_node.parent_node.features
            else:
                self.features = set()
        else:
            self.features = set()
        
        self.features.add(self.feature_number)
              
    def add_child(self, child):
        self.child_nodes.append(child)
        
    def get_class(self, vector, count):
        feature_value = vector[self.feature_number]
        
        count+=1
        
        for i in range(len(self.child_nodes)):
            if feature_value <= self.child_nodes[i].feature_value or self.feature_number == -1:
                return self.child_nodes[i].child_node.get_class(vector, count)
            
        
class ValueNode:
    def __init__(self, feature_value):
        
        
        self.feature_value = feature_value
        
    
    def add_parent(self, parent_node):
        self.parent_node = parent_node
        self.feature_number = self.parent_node.feature_number
            
    def add_child(self, child):
        self.child_node = child
        
class LeafNode:
    def __init__(self, observations):
        
        self.observations = observations
        
        observation_classes = set()
        for observation in observations:
            observation_classes.add(observation[1])
        
        if len(observation_classes) > 1:
            self.homogeneous = False
        else:
            self.homogeneous = True
            try:
                self.klass = observations[0][1]
            except:
                self.klass = '1'
    
    def add_parent(self, parent_node):
        self.parent_node = parent_node
    
    def get_class(self, vector, count):
        count+=1
        return self.klass

def simplifiedObservations(observations):
    
    simplified_o = list()
    
    for i in range(len(observations)):
        simplified_o.append((i, observations[i].klass))
        
    return simplified_o

def predict1(root_node, observations, count=0):
    if count>5:
        return
    count+=1
    
    if isinstance(root_node, FeatureNode):
        print(root_node)
        print("parent_node: "+str(root_node.parent_node))
        for child_node in root_node.child_nodes:
            print("child_node of "+str(root_node)+" is "+str(child_node))
            predict1(child_node, observations, count) 
    elif isinstance(root_node, ValueNode):
        print("parent_node: "+str(root_node.parent_node))
        print("child_node: "+str(root_node.child_node))
        predict1(root_node.child_node, observations, count)
    elif isinstance(root_node, LeafNode):
        print("leaf node")
        print("parent_node: "+str(root_node.parent_node))

    
def train(observations_list):
    
    '''
    Creates a tree from the smpl_observations, using the decision tree algorithm in 
    Data Management for Multimedia Retrieval, Chapter 9, section 9.1
    
    '''
    
    #1. Initially crete a root node and a single leaf node and assign all smpl_observations
    #to the leaf node
    smpl_observations = simplifiedObservations(observations_list)
    heterogeneous_leaves = deque()
    
    root_node_feat = FeatureNode(-1)
    root_node_feat.add_parent(None)
    
    root_node_val = ValueNode(None)
    root_node_val.add_parent(root_node_feat)
    root_node_feat.add_child(root_node_val)
    
    root_node_leaf = LeafNode(smpl_observations)
    root_node_val.add_child(root_node_leaf)
    root_node_leaf.add_parent(root_node_val)
    
    if not root_node_leaf.homogeneous:
        heterogeneous_leaves.append(root_node_leaf)
    
    #2. While there are still leaf nodes with heterogeneous labels:
    
    count = 0
    while not len(heterogeneous_leaves) == 0:
        
        count += 1
        #2.1 Pick a leaf node with a set, L, of heterogenous class labels and
        #removes the leaf node from the tree
        leaf_node = heterogeneous_leaves.popleft()
        smpl_observations = leaf_node.observations
        value_node = leaf_node.parent_node
        value_node.child_node = None
        del leaf_node   
        
        #2.2 Pick a feature dimension, f_i, and associate this as the 
        #decision attribute to the parent of the eliminated leaf.
        
        #best_feature = next_best_feature(sorted_features_l, value_node.parent_node.features)
        
        best_feature = information_gain_entropy(smpl_observations, observations_list, value_node.parent_node.features)
        
        new_feature_node = FeatureNode(best_feature)
        new_feature_node.add_parent(value_node)
        value_node.add_child(new_feature_node)
        
        
            
        #2.3 For each distinct value of the feature:
        values = distinct_values(smpl_observations, observations_list, best_feature)
        
        
        added_o_v = set()
        
        for v in values:
            #2.3.1 - Create a new child node under the feature node
            new_value_node = ValueNode(v)
            new_value_node.add_parent(new_feature_node)
            new_feature_node.add_child(new_value_node)
            #2.3.2 - Create a new leaf node under v and associate the subset of
            #smpl_observations that have value v as the smpl_observations of this new leaf.
            o_v = filter_observations_value(smpl_observations, observations_list, best_feature, v, added_o_v)
            added_o_v.update(o_v)
            new_leaf_node = LeafNode(o_v)
            new_leaf_node.add_parent(new_value_node)
            new_value_node.add_child(new_leaf_node)
            
            
            if not new_leaf_node.homogeneous:
                heterogeneous_leaves.append(new_leaf_node)
        
        
        del smpl_observations
    
    decision_tree_root = root_node_feat
    return decision_tree_root

def next_best_feature(sorted_features_l, used_already_features_s):
    '''
    sorted_features is a list that contains the features in sorted order of discriminative power
    used_already_features_s is a set that contains the features that has already been used
    The function returns the best feature in the list of sorted_features that is not in the set of used_already features
    '''
    
    for a in sorted_features_l:
        if a not in used_already_features_s:
            return a
    
    return None

def information_gain_entropy_sorted(smpl_observations, observations, features_to_ignore):
    '''
    returns the index of the most discriminative feature according to the
    information gain by impurity measure.
    The first parameter is the smpl_observations where we measure the features_to_ignore.
    The second parameter is a set of features_to_ignore to be ignored in the calculations 
    '''
    
    if len(smpl_observations)<=0: 
        return -1
    
    from operator import itemgetter
    
    #1 Calculating entropy of smpl_observations
    entropy_O = entropy(smpl_observations)
    
    #2. Calculating summation of entropies of subsets of smpl_observations
    total_features = set(range(len(observations[0].vector)))
    features_to_use = total_features - features_to_ignore
    
    feature_score_l = list()
    
    for feature in features_to_use:
        feature_values = set()
        
        for observation in smpl_observations:
            vector = observations[observation[0]].vector
            feature_values.add(vector[feature])
        
        summation = 0
        for value in feature_values:
            observations_sublist = list()
            for observation in smpl_observations:
                vector = observations[observation[0]].vector
                if vector[feature] == value:
                    observations_sublist.append(observation)
            
            summation += (len(observations_sublist)/len(smpl_observations))*entropy(observations_sublist)
        
        feature_score_l.append((feature, entropy_O - summation))
    
    sorted_scores = sorted(feature_score_l, key = itemgetter(1))
    
    to_return = list()
    for a in sorted_scores:
        to_return.append(a[0])
    
    return to_return
    
def information_gain_entropy(smpl_observations, observations, features_to_ignore):
    
    '''
    returns the index of the most discriminative feature according to the
    information gain by impurity measure.
    The first parameter is the smpl_observations where we measure the features_to_ignore.
    The second parameter is a set of features_to_ignore to be ignored in the calculations 
    '''
    
    if len(smpl_observations)<=0: 
        return -1
    
    from operator import itemgetter
    
    #1 Calculating entropy of smpl_observations
    entropy_O = entropy(smpl_observations)
    
    #2. Calculating summation of entropies of subsets of smpl_observations
    total_features = set(range(len(observations[0].vector)))
    features_to_use = total_features - features_to_ignore
    
    feature_score_l = list()
    
    for feature in features_to_use:
        feature_values = set()
        
        for observation in smpl_observations:
            vector = observations[observation[0]].vector
            feature_values.add(vector[feature])
        
        summation = 0
        for value in feature_values:
            observations_sublist = list()
            for observation in smpl_observations:
                vector = observations[observation[0]].vector
                if vector[feature] == value:
                    observations_sublist.append(observation)
            
            summation += (len(observations_sublist)/len(smpl_observations))*entropy(observations_sublist)
        
        feature_score_l.append((feature, entropy_O - summation))
    
    sorted_scores = sorted(feature_score_l, key = itemgetter(1))
    return sorted_scores[0][0]
    
    
def entropy(observations):
    
    if len(observations) <= 0:
        return 0
    
    entropy = 0
    
    class_count_d = dict()
    for observation in observations:
        if observation[1] in class_count_d:
            class_count_d[observation[1]] += 1
        else:
            class_count_d[observation[1]] = 1
    
    for key in class_count_d:
        
        entropy += (class_count_d[key]/len(observations))*math.log(class_count_d[key]/len(observations))
    
    return entropy

def filter_observations_value(smpl_observations, observations_list, feature, value, added_o_v):
    '''
    Returns a sublist of smpl_observations that have value value under
    feature feature
    '''
    observations_sublist = list()
    for observation in smpl_observations:
        features = observations_list[observation[0]].vector
        if (len(features) > feature):
            feat_value = features[feature]
            if feat_value <= value and observation not in added_o_v:
                observations_sublist.append(observation)
    
    return observations_sublist
        
def distinct_values(o, observations_list, f):
    '''
    Returns the list of distinct values of feature f in observations o, sorted 
    '''
    
    distinct_values_s = set()
    for observation in o:
        features = observations_list[observation[0]].vector
        if (len(features) > f):
            
            distinct_values_s.add(features[f])
    
    distinct_values_l = list(distinct_values_s)
    distinct_values_l.append(sys.maxsize)
    distinct_values_l.sort()
    
    return distinct_values_l


def predict(decision_tree_root, observations):
    '''
    predicts the class of the given observations and returns a list with the class
    '''
    if decision_tree_root == None:
        
        return
    
    classes = list()
    count = 0
    for observation in observations:
        vector = observation.vector
        classes.append(decision_tree_root.get_class(vector, 0))
        
    return classes
    

def main():
    train(1)
    
if __name__ == "__main__":
    main()
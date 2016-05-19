#!/usr/bin/env python
#coding=utf-8
from __future__ import absolute_import

import json
import logging
import argparse
import math
import sys
import os
from pyltp import Segmentor

# add search path for preprocessing_config
preprocessing_dir_path = os.path.normpath(os.path.split(os.path.abspath(__file__))[0] + "/../preprocessing")
sys.path.append(preprocessing_dir_path)
from preprocessing_config import CWS_MODEL_PATH

logging.basicConfig(level=logging.INFO)
class DocStruct(object) :
    def __init__(self , doc_id) :
        self.doc_id = doc_id
        self.query_words_info = {}
    def add_query_words(self , word_id , posting_item) :
        self.query_words_info[word_id] = posting_item
    def get_nr_hit_words(self) :
        return len(self.query_words_info)
    def _get_field_tf_info(self , key) :
        field_info = []
        for word_id , posting_item in self.query_words_info.items() :
            field_tf = posting_item[key]
            field_info.append( (word_id , field_tf) )
        return field_info

    def get_title_word_tf_info(self ) :
        return self._get_field_tf_info("title_tf")

    def get_content_word_tf_info(self) :
        return self._get_field_tf_info("content_tf")

class SearchStruct(object) :
    def __init__(self , doc_data , words_dict , inverted_index) :
        self.search_doc_results = {}
        self.words_idf = {}
        self.doc_len = {}
        self.doc_data = doc_data
        self.words_dict = words_dict
        self.inverted_index = inverted_index
        self.total_docs_num = len(doc_data)
        

    def add_search_keyword(self , word_id) :
        posting_list = self.inverted_index[word_id]["posting_list"] # word id must in inverted_index
        for posting_item in posting_list :
            doc_id = posting_item['doc_id']
            if doc_id not in self.search_doc_results :
                self.search_doc_results[doc_id] = DocStruct(doc_id)
            self.search_doc_results[doc_id].add_query_words(word_id , posting_item)
            if doc_id not in self.doc_len :
                doc_struct = self.doc_data[doc_id]
                self.doc_len[doc_id] = { 'title_len' : len(doc_struct["title"]) , "content_len" : len(doc_struct["content"])  }
        self.words_idf[word_id] = math.log( self.total_docs_num / (float(self.inverted_index[word_id]['doc_freq']) + 1) + 1)
    
    def _sort_result(self) :
        scores = []
        for doc_id , doc_struct in self.search_doc_results.items() :
            # coor score
            coor_score = doc_struct.get_nr_hit_words()
            # title filed score
            title_field_score = 0.
            title_field_info = doc_struct.get_title_word_tf_info()
            print title_field_info
            for word_id , tf in title_field_info :
                title_field_score += tf * ( self.words_idf[word_id] ** 2 )
            title_length_norm = 1. / math.sqrt(self.doc_len[doc_id]["title_len"])
            title_field_score *= title_length_norm
            # content field score
            content_field_score = 0.
            content_field_info = doc_struct.get_content_word_tf_info()
            for word_id , tf in content_field_info :
                content_field_score += tf * ( self.words_idf[word_id] **2 )
            content_length_norm = 1. / math.sqrt(self.doc_len[doc_id]["content_len"])
            content_field_score *= content_length_norm
            # final   
            final_score = coor_score * ( title_field_score + content_field_score )
            print coor_score
            print title_field_score
            print content_field_score

            scores.append((doc_id , final_score))
        return sorted(scores , key=lambda x : x[1] , reverse=True)

    def get_result(self) :
        sorted_docs_and_score = self._sort_result()
        return sorted_docs_and_score

class SearchEngine(object) :
    def __init__(self , cws_model_path=CWS_MODEL_PATH) :
        self.doc_data = None
        self.words_dict = None
        self.inverted_index = None
        self.segmentor = Segmentor()
        self.segmentor.load(cws_model_path)

    def _load_json_data(self , in_path) :
        with open(in_path) as f :
            return json.load(f)
    
    def load_data(self , doc_path , words_dict_path , inverted_index_path) :
        logging.info("loading data...")
        self.doc_data = self._load_json_data(doc_path)
        self.words_dict = self._load_json_data(words_dict_path)
        self.inverted_index = self._load_json_data(inverted_index_path)
        logging.info("done.")
    
    def _parse_query(self , query) :
        words = self.segmentor.segment(query)
        words = list(words)
        query_words_id = []
        for word in words :
            word = word.decode("utf-8") # words_dict , doc_data , inverted_index all are unicode
            if self.words_dict.has_key(word) :
                query_words_id.append(self.words_dict[word])
        return query_words_id
    
    def _get_result(self , query_words) :
        search_result = SearchStruct(self.doc_data , self.words_dict , self.inverted_index)
        for word_id in query_words :
            search_result.add_search_keyword(word_id)
        return search_result.get_result()

            
    def search(self , query) :
        query_words_id = self._parse_query(query.strip())
        print query_words_id
        result = self._get_result(query_words_id)
        print result


if __name__ == "__main__" :
    argp = argparse.ArgumentParser(description="Search Engine")
    argp.add_argument("--doc_data" , help="path to doc data" , type=str , required=True)
    argp.add_argument("--words_dict" , help="path to words dict" , type=str , required=True )
    argp.add_argument("--inverted_index" , help="path to inverted index file" , type=str , required=True)
    args = argp.parse_args()
    engine = SearchEngine()
    engine.load_data(args.doc_data , args.words_dict , args.inverted_index)
    while True :
        try :
            line = sys.stdin.readline()
            line = line.strip()
            if line == "" :
                break
        except EOFError , e :
            break
        engine.search(line)


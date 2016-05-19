#!/usr/bin/env python
#coding=utf8

import json
import logging
import argparse

logging.basicConfig(level=logging.INFO)

class WordPosting(object) :
    def __init__(self) :
        self.doc_freq = 0
        self.posting_list = []

    def update_doc_freq(self) :
        self.doc_freq = len(self.posting_list)

    def append_posting_item(self , posting_item) :
        self.posting_list.append(posting_item)
        self.update_doc_freq()

    def get_data(self) :
        return {
                "doc_freq" : self.doc_freq ,
                "posting_list" : [ posting_item.get_data() for posting_item in self.posting_list  ]
                }

class PostingItem(object) :
    def __init__(self , doc_id) :
        self.doc_id = doc_id
        self.title_tf = 0
        self.title_pos = []
        self.content_tf = 0
        self.content_pos = []

    def update_title_tf(self) :
        self.title_tf = len(self.title_pos)

    def add_title_pos(self , title_pos) :
        self.title_pos.append(title_pos)
        self.update_title_tf()

    def update_content_tf(self) :
        self.content_tf = len(self.content_pos)

    def add_content_pos(self , content_pos) :
        self.content_pos.append(content_pos)
        self.update_content_tf()
    
    def get_data(self) :
        return {
                'doc_id' : self.doc_id ,
                'title_tf' : self.title_tf ,
                'title_pos' : self.title_pos ,
                'content_tf' : self.content_tf ,
                "content_pos" : self.content_pos
                }

class InvertedIndexFactory(object) :
    def __init__(self) :
        self.inverted_index = None

    def _load_json_data(self , path) :
        with open(path) as f :
            return json.load(f)

    def _make_doc_posting_items(self , doc_id , doc_data , words_dict) :
        doc_posting = dict()
        title_words = doc_data['title']
        content_words = doc_data['content']
        title_pos = 0
        for word in title_words :
            # filter and get word id
            if word not in words_dict :
                continue
            word_id = words_dict[word]
            if word_id not in doc_posting :
                doc_posting[word_id] = PostingItem(doc_id)
            doc_posting[word_id].add_title_pos(title_pos)
            title_pos += 1
        content_pos = 0
        for word in content_words :
            if word not in words_dict :
                continue
            word_id = words_dict[word]
            if word_id not in doc_posting :
                doc_posting[word_id] = PostingItem(doc_id)
            doc_posting[word_id].add_content_pos(content_pos)
            content_pos += 1
        return doc_posting
    
    def _update_inverted_idx(self , doc_posting_item) :
        for word_id , posting_item in doc_posting_item.items() :
            assert(word_id < len(self.inverted_index))
            self.inverted_index[word_id].append_posting_item(posting_item)
    
    def _get_data(self) :
        return [ word_posting_list.get_data() for word_posting_list in self.inverted_index ]
    
    def _check_data(self) :
        '''
        DEBUG OUTPUT 
        '''
        for word_posting in self.inverted_index :
            for posting_item in word_posting.posting_list :
                if posting_item.title_tf != 0 :
                    print posting_item.title_tf

    def make_inverted_index(self , doc_path , words_dict_path) :
        logging.info("making inverted index ...")
        doc_dict = self._load_json_data(doc_path)
        words_dict = self._load_json_data(words_dict_path) # words id is start from 0 !
        self.inverted_index = [ WordPosting() for i in range(len(words_dict.keys())) ]
        for doc_id , doc_data in doc_dict.items() :
            doc_posting = self._make_doc_posting_items(doc_id , doc_data , words_dict)
            self._update_inverted_idx(doc_posting)
        logging.info('done.')

    def save_inverted_index(self , to_path) :
        logging.info("saving inverted index ...")
        with open(to_path , "w") as of :
            json.dump(self._get_data() , of , ensure_ascii=False)
        logging.info('done.')

if __name__ == "__main__" :
    argp = argparse.ArgumentParser(description="make inverted index")
    argp.add_argument("--doc_data" , help="path to doc json data" , type=str , required=True)
    argp.add_argument("--words_dict" , help="path to words dict json data" , type=str , required=True)
    argp.add_argument("--output" , "-o" , help="path to output of inverted index" , type=str , required=True)
    args = argp.parse_args()
    iif = InvertedIndexFactory()
    iif.make_inverted_index(args.doc_data , args.words_dict)
    iif.save_inverted_index(args.output)

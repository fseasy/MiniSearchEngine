#!/usr/bin/env python
#coding=utf-8

import json
import re
import logging
import os
from pyltp import Segmentor
from preprocessing_config import CWS_MODEL_PATH , STOP_WORDS_DIR

logging.basicConfig(level=logging.INFO)

class PreProcessor(object) :
    def __init__(self , cws_model_path=CWS_MODEL_PATH , stop_words_dir=STOP_WORDS_DIR) :
        self.raw_data = None
        self.processed_data = None
        self.words_dict = None
        self.STOP_WORDS = self._load_stop_words(stop_words_dir) 
        self.segmentor = Segmentor()
        self.segmentor.load(cws_model_path)

    def _load_stop_words(self , dir_name) :
        stop_words = set()
        cur_abs_dir_path = os.path.split(os.path.abspath(__file__))[0]
        dir_path = os.path.join(cur_abs_dir_path , dir_name)
        for file_name in os.listdir(dir_path) :
            file_path = os.path.join(dir_path , file_name) 
            with open(file_path) as f :
                for line in f :
                    word = line.strip()
                    stop_wrods.add(word)
        return stop_words

    def load_raw_data(self , path) :
        with open(path) as f :
            self.raw_data = json.load(f)
    
    def split_sentence(self , content) :
        '''
        split content to sentence
        '''
        sents = []
        for paragraph in sents :
            split_rst = re.split(ur"[,.?'。，？！“” ]+" , paragraph) # has space 
            sents.extend(split_rst)
        return sents
    
    def segment(self , unicode_line) :
        '''
        return : list of words
        '''
        utf8_line = unicode_line.strip().encode("utf8")
        words = self.segmentor.segment(utf8_line)
        return words
    
    def make_doc_data(self , url , title_seged , sents_seged) :
        return { 'url' : url ,
                 'title' : title_seged ,
                 'content' : sents_seged
                 }

    def add_word2words_dict(self , words) :
        for word in words :
            if word not in self.STOP_WORDS :
                self.words_dict.add(word)

    def do_preprocess(self) :
        logging.info("do preprocessing ...")
        self.processed_data = dict()
        self.words_dict = set()
        for page_id , page_data in raw_data.items() :
            url = page_data['url']
            title = page_data["title"]
            content = page_data["content"]
            sents = self.split_sentence(content)
            # segment
            title_seged_list = self.segment(title)
            sents_seged_list = []
            for sent in sents :
                sents_seged_list.append(self.segment(sent))
            self.processed_data[page_id] = self.make_doc_data(url , title_seged_list , sents_seged_list)
        logging.info('done.')
    
    def save_doc_data(self , to_path) :
        with open(to_path , 'w') as of:
            json.dump(self.processed_data , of)

    def save_words_dict(self , to_path) :
        with open(to_path , 'w') as of :
            json.dump(self.words_dict , of)

if __name__ == "__main__" :


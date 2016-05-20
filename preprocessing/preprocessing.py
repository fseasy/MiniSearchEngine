#!/usr/bin/env python
#coding=utf-8

import json
import re
import logging
import os
import argparse
from pyltp import Segmentor
from preprocessing_config import CWS_MODEL_PATH , STOP_WORDS_DIR , SENT_SPLIT_SYMBOLS

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
                    stop_words.add(word)
        for symbol in SENT_SPLIT_SYMBOLS :
            stop_words.add(symbol)
        return stop_words

    def load_raw_data(self , path) :
        with open(path) as f :
            self.raw_data = json.load(f)
    
    def _split_sentence(self , content) :
        '''
        split content to sentence
        '''
        sents = []
        paras = content.split("\n")
        for paragraph in paras :
            split_rst = re.split(ur"[%s]+" %(SENT_SPLIT_SYMBOLS) , paragraph) # has space 
            sents.extend(split_rst)
        return sents
    
    def _segment(self , unicode_line) :
        '''
        return : list of words
        '''
        utf8_line = unicode_line.strip().encode("utf8")
        words = list(self.segmentor.segment(utf8_line))
        return words
    
    def _make_doc_data(self , url , title_seged , sents_seged) :
        return { 'url' : url ,
                 'title' : title_seged ,
                 'content' : sents_seged
                 }

    def _add_word2words_dict(self , words) :
        for word in words :
            if word not in self.STOP_WORDS :
                word = word.lower() 
                self.words_dict.add(word)

    def do_preprocessing(self) :
        logging.info("do preprocessing ...")
        self.processed_data = dict()
        self.words_dict = set()
        for page_id , page_data in self.raw_data.items() :
            url = page_data['url']
            title = page_data["title"]
            content = page_data["content"]
            sents = self._split_sentence(content)
            # segment
            title_words = self._segment(title)
            content_words = []
            for sent in sents :
                content_words.extend(self._segment(sent))
                content_words.append(" ") # another space to avoid that they become one line when merging at output snippet 
            self.processed_data[page_id] = self._make_doc_data(url , title_words , content_words)
            self._add_word2words_dict(title_words + content_words)
        logging.info('done.')
    
    def save_doc_data(self , to_path) :
        logging.info("saving doc data to ` %s `" %(to_path) )
        with open(to_path , 'w') as of:
            json.dump(self.processed_data , of )
        logging.info("done.")

    def save_words_dict(self , to_path) :
        logging.info("saving words dict to ` %s `" %(to_path))
        words_list = list(self.words_dict)
        words_dict = {word : word_id for word_id , word in enumerate(words_list) }
        with open(to_path , 'w') as of :
            json.dump(words_dict , of , ensure_ascii=False) # json not support `set`
        logging.info("done.")

if __name__ == "__main__" :
    argp = argparse.ArgumentParser(description="Preprocessing module for MiniSearchEngin")
    argp.add_argument("--raw_data" , type=str , required=True , help="path to spider result .")
    argp.add_argument("--doc_path" , type=str , required=True , help="path to processing result of doc data")
    argp.add_argument("--words_path" , type=str , required=True , help="path to processing result of words dict path")
    args = argp.parse_args()
    processor = PreProcessor() 
    processor.load_raw_data(args.raw_data)
    processor.do_preprocessing()
    processor.save_doc_data(args.doc_path) 
    processor.save_words_dict(args.words_path)

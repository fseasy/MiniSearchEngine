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
    
    def get_hit_words(self) :
        return self.query_words_info.keys()
    
    def _get_field_info(self , key) :
        field_info = []
        for word_id , posting_item in self.query_words_info.items() :
            field_value = posting_item[key]
            field_info.append( (word_id , field_value) )
        return field_info
    
    def get_content_word_pos_info(self) :
        rst = self._get_field_info("content_pos")
        return [ rst_tuple[1] for rst_tuple in rst ]

    def get_title_word_tf_info(self ) :
        return self._get_field_info("title_tf")

    def get_content_word_tf_info(self) :
        return self._get_field_info("content_tf")

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

            scores.append((doc_id , final_score , doc_struct.get_hit_words() , doc_struct.get_content_word_pos_info()))
        return sorted(scores , key=lambda x : x[1] , reverse=True)

    def get_result(self) :
        sorted_docs_and_score = self._sort_result()
        return sorted_docs_and_score
    
    def get_doc_posting_of_query(self , doc_id) :
        self.search_doc_results.get(doc_id , None)

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
        self.id2words = [ "" ] * len(self.words_dict)
        for word , word_id in self.words_dict.items() :
            self.id2words[word_id] = word
        logging.info("done.")
    
    def _parse_query(self , query) :
        query_words = query.split()
        words = []
        for query_word in query_words :
            words.extend(list(self.segmentor.segment(query_word)))
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
    
    def _generate_snippet(self , content_pos , doc_id) :
        if len(content_pos) == 0 : return ""
        words_first_pos = [ word_pos[0] for word_pos in content_pos ]
        words_first_pos = sorted(words_first_pos)
        ranges = []
        doc_content_len = len(self.doc_data[doc_id]["content"])
        window_size = 20
        for first_pos in words_first_pos :
            start_pos = max(first_pos - window_size,0)
            end_pos = min(first_pos + window_size , doc_content_len)
            ranges.append([start_pos , end_pos])
        merge_ranges = [ ranges[0] ]
        for i in range(1 , len(ranges)) :
            start_pos = ranges[i][0]
            end_pos = ranges[i][1]
            last_range_end_pos = merge_ranges[-1][1]
            if start_pos < last_range_end_pos :
                merge_ranges[-1][1] = end_pos
            else :
                merge_ranges.append(ranges[i])
        content = self.doc_data[doc_id]["content"]
        lines = []
        for start_pos , end_pos in merge_ranges :
            lines.append( "".join(content[start_pos : end_pos+1]) )
        return "...".join(lines)


    def prettify_output(self , result) :
        '''
        result : [ ( doc_id , final_score , [hit_words1 , ...] , [content_pos1 , ...]) , ... ]
        '''
        index = 1 
        output_str_list = []
        for (doc_id , final_score , hit_words , content_pos) in result :
            cur_doc_data = self.doc_data[doc_id]
            doc_output = []
            hit_words_str = [ self.id2words[word_id] for word_id in hit_words ]
            doc_output.append(u"第 %s 条结果， 包含搜索词 [ %s ] , 排序分数 %.6f "%(index , " ] [ ".join(hit_words_str) , final_score  ))
            doc_output.append(u"标题 : %s" %("".join(cur_doc_data["title"])))
            # abstract context
            # i have no idea about the algorithm , so just return the first pos
            snippet = self._generate_snippet(content_pos , doc_id)
            if snippet != "" :
                doc_output.append(u"摘要 : ... %s ..." %( snippet ))
            else :
                doc_output.append(u"摘要 : 无")
            doc_output.append(u"url : %s" %(self.doc_data[doc_id]["url"]) )
            output_str_list.append("\n".join(doc_output))
            index += 1
        return output_str_list
            
    def search(self , query) :
        query_words_id = self._parse_query(query.strip())
        result = self._get_result(query_words_id)
        output_str_list = self.prettify_output(result)
        return output_str_list

if __name__ == "__main__" :
    argp = argparse.ArgumentParser(description="Search Engine")
    argp.add_argument("--doc_data" , help="path to doc data" , type=str , required=True)
    argp.add_argument("--words_dict" , help="path to words dict" , type=str , required=True )
    argp.add_argument("--inverted_index" , help="path to inverted index file" , type=str , required=True)
    args = argp.parse_args()
    engine = SearchEngine()
    engine.load_data(args.doc_data , args.words_dict , args.inverted_index)
    page_item_num = 10
    print 
    print "%s MiniSearchEngin %s" %("*"*20 , "*"*20)
    while True :
        print
        print "请输入查询语句： " ,
        try :
            line = sys.stdin.readline()
            line = line.strip()
            if line == "" :
                break
        except EOFError , e :
            break
        result_list = engine.search(line)
        total_items_num = len(result_list)
        
        print 
        if total_items_num == 0 :
            print "\n无搜索结果\n"
            continue
        page_num = int(math.ceil(total_items_num / float(page_item_num)))
        for page_id in range(page_num) :
            start_idx = page_id * page_item_num
            end_idx = min((page_id + 1) * page_item_num , total_items_num )
            pages = "\n\n".join( result_list[start_idx:end_idx] )
            print pages.encode("utf-8")
            print "第 %s/%s 页内容," %(page_id+1 , page_num) ,
            if page_id == page_num -1 :
                print " 最后一页，按任意键开始下次搜索..."
                sys.stdin.readline()
                break
            else :
                print " [n] 下一页 [s] 停止显示，开始下次搜索"
                response = sys.stdin.readline().strip()
                if response.lower() != "n" :
                    break
        

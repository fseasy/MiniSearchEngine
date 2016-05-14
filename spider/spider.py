#!/usr/bin/env python
#coding=utf8

import sys
import urllib
import urllib2
import Queue
import re
import logging
import StringIO
import gzip
import json
from bs4 import BeautifulSoup

from spider_conf import ( SPIDER_BASE_URL , 
                          SPIDER_URL_SPECIFIC_KEY , 
                          SPIDER_CNT_LIMIT)

logging.basicConfig(level=logging.INFO)

class Spider(object) :
    def __init__(self ,
            base_url=SPIDER_BASE_URL , 
            white_url_key=SPIDER_URL_SPECIFIC_KEY , 
            crawling_cnt_limit=SPIDER_CNT_LIMIT) :
        
        self.base_url = base_url
        self.white_url_key = SPIDER_URL_SPECIFIC_KEY
        self.crawling_cnt_limit = crawling_cnt_limit
        self.crawl_result = dict()
        self.seen_urls_set = set()
        self.crawling_request_data = {
                "headers" : {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko" ,
                              "Accept-Encoding": "gzip, deflate",
                              "Accept-Language": "zh-Hans-CN, zh-Hans; q=0.8, en-US; q=0.5, en; q=0.3" ,
                              } 
            }

    def crawl_page(self , url) :
        request = urllib2.Request(url , **self.crawling_request_data)
        response = urllib2.urlopen(request)
        response_cont = response.read()
        if response.info().get("Content-Encoding") == 'gzip' :
            buf = StringIO.StringIO(response_cont)
            f = gzip.GzipFile(fileobj=buf)
            response_cont = f.read()
        return response_cont

    def abstract_content(self , soup) :
        content = []
        for line in soup.stripped_strings :
            line = line.strip()
            if re.match(ur"^[\u0000-\u0100]+$" , line) :
                continue # may be js code
            if len(line) < 10 :
                continue
            content.append(line)
        return u"\n".join(content)

    def abstract_all_urls_from_soup(self , soup) :
        tag_a_list = soup.body.find_all("a")
        url_set = set()
        for tag_a_node in tag_a_list :
            href = tag_a_node.get("href")
            if href :
                # some href is not link , such as 
                # <a href="javascript:void(0)" .. >
                # <a href="#"> , but some url like this : http://xxxx.html#anchor
                if href.find("javascript") != -1 : continue
                if href.startswith("#") : continue
                if href.rfind("#") != -1 :
                    href = href[:href.rfind("#")]
                url_set.add(href)
        return list(url_set)

    def processing_page(self , raw_page_content) :
        soup = BeautifulSoup(raw_page_content , "lxml")
        url_list = self.abstract_all_urls_from_soup(soup)
        content = self.abstract_content(soup)
        return url_list , content

    def filter_seen_urls(self , url_list) :
        unseen_list = []
        for url in url_list :
            if url not in self.seen_urls_set :
                unseen_list.append(url)
        return unseen_list

    def update_seen_url_set(self , url_list) :
        for url in url_list :
            self.seen_urls_set.add(url)

    def filter_specific(self , url_list) :
        filtered_url_list = []
        for url in url_list :
            if url.find(self.white_url_key) == -1 : # only when page has white_url_key that it will be processed
                continue
            if url.find("?") != -1 : 
                continue
            filtered_url_list.append(url)
        return filtered_url_list

    def save_data(self , write_path) :
        json_data = json.dumps(self.crawl_result , indent=4 , separators=(",",":"))
        with open(write_path , 'w') as f:
            f.write(json_data)

    def crawl(self) :
        crawling_queue = Queue.LifoQueue() # LIFO ,  stack
        crawling_queue.put(self.base_url)
        crawling_cnt = 0
        while not crawling_queue.empty() :
            target_url = crawling_queue.get() 
            logging.info("crawling url : %s [%s]" %(target_url , crawling_cnt+1) )
            try :
                page_content = self.crawl_page(target_url)
                all_url_list , content = self.processing_page(page_content)
            except :
                logging.warn("crawling error . skipped")
                continue
            crawling_cnt += 1
            # storing
            self.crawl_result[target_url] = content 
            # udpate crawl queue
            unseen_urls = self.filter_seen_urls(all_url_list)
            self.update_seen_url_set(unseen_urls)
            white_urls = self.filter_specific(unseen_urls)
            for url in white_urls :
                crawling_queue.put_nowait(url)
            if crawling_cnt >= self.crawling_cnt_limit :
                break

if __name__ == "__main__" :
    if len(sys.argv) < 2 :
        print >> sys.stderr , "usage : %s [output_path]" %(sys.argv[0])
        exit(1)
    s = Spider()
    s.crawl()
    s.save_data(sys.argv[1])

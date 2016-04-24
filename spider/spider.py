#!/usr/bin/env python
#coding=utf8

import urllib
import urllib2
import Queue
import re
from bs4 import BeautifulSoup

def abstract_content(soup) :
    content = []
    for line in soup.stripped_strings :
        line = line.strip()
        if re.match(ur"^[\u0000-\u0100]+$" , line) :
            continue # may be js code
        if len(line) < 10 :
            continue
    return u"\n".join(content)

def abstract_all_urls_from_soup(soup) :
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

def processing_page(raw_page_content) :
    soup = BeautifulSoup(raw_page_content , "lxml")
    url_list = abstract_all_urls_from_soup(soup)
    content = abstract_content(soup)
    return url_list , content

def get_unseen_urls_and_udpate_seen_set(url_list , seen_set) :
    unseen_list = []
    for url in url_list :
        if url not in seen_set :
            unseen_list.append(url)
            seen_set.add(url)
    return unseen_list

def crawler() :
    target_base_url = "http://www.hupu.com"
    crawling_queue = Queue.LifoQueue() # LIFO ,  stack
    crawling_queue.put(target_base_url)
    while not crawling_queue.empty() :
        target_url = crawling_queue.get() 
        request = urllib2.Request(target_url)
        response = urllib2.urlopen(request)
        page_content = response.read()
        soup = BeautifulSoup(page_content , "lxml")
        url_list = soup.find_all("a")
        text = abstract_useful_content(soup.stripped_strings)
        print url_list
        print 
        print text.encode("utf8")


if __name__ == "__main__" :
    page_content = open("index.html").read()
    url_list , content = processing_page(page_content)
    storing_data = {'url' : "index.html" , "content" : content}
    seen_url_set = set()
    unseen_url_list = get_unseen_urls_and_udpate_seen_set(url_list , seen_url_set )
    

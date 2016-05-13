#!/usr/bin/env python
#coding=utf8

import urllib
import urllib2
import Queue
import re
from bs4 import BeautifulSoup

def abstract_useful_content(content_generator) :
    content = []
    for line in content_generator :
        line = line.strip()
        if re.match(ur"^[\u0000-\u0100]+$" , line) :
            continue # may be js code
        if len(line) < 9 :
            continue
        else :
            print line.encode("utf8")
            print len(line)
        content.append(line)
    return u'\n'.join(content)


if __name__ == "__main__" :
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

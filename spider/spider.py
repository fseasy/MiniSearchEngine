#!/usr/bin/env python
#coding=utf8

import urllib
import urllib2

if __name__ == "__main__" :
    target_base_url = "http://www.hupu.com"
    request = urllib2.Request(target_base_url)
    response = urllib2.urlopen(request)
    print response.read()

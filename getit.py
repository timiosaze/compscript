from urllib import response
from bs4 import BeautifulSoup
import time
import math
from pathlib import Path
import certifi
import urllib3
import requests
from urllib3 import ProxyManager, make_headers
from urllib.request import Request, urlopen
import mysql.connector
from urllib.parse import urlparse
from fake_useragent import UserAgent
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import concurrent.futures
import random
from requests.auth import HTTPProxyAuth


from deep_translator import (GoogleTranslator,
                             MicrosoftTranslator,
                             PonsTranslator,
                             LingueeTranslator,
                             MyMemoryTranslator,
                             YandexTranslator,
                             PapagoTranslator,
                             DeeplTranslator,
                             QcriTranslator,
                             single_detection,
                             batch_detection)
ua = UserAgent()
chrome_ua = ua.google

def clear_txt():
    f = open('yes.txt', 'r+')
    f.truncate(0) # need '0' when using r+
    

def proxies_list():
    headers={'User-Agent': chrome_ua}
    response = requests.get('https://raw.githubusercontent.com/fate0/proxylist/master/proxy.list', headers=headers)
    with open("yes.json", "w") as f:
        x = response.text.replace("}", "},")
        string = x.strip()[:-1]
        arr = ''.join(('[',string,']'))
        j = json.loads(arr)
        json.dump(j, f)
        f.close()

def proxies_arr():
    proxies_arr = []
    with open('response.txt', 'r') as reader:
        for line in reader.readlines():
            # print(line, end='')
            proxies_arr.append(line.strip())
    return proxies_arr

def jsonformat():
    f = open('yes.json')
  
    # returns JSON object as 
    # a dictionary
    data = json.load(f)
    arr = []
    for x in data:
       line = x["host"] + ":" + str(x["port"])
       arr.append(line)
    # print(arr)
    with open("yes.txt", "w") as file:
        file.write('\n'.join(arr))
        file.close()
clear_txt()
# proxies_list()

jsonformat()
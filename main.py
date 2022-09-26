from re import I
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

# MYSQL CONNECTION PARAMS
cnx = mysql.connector.connect(host='localhost', user='python', password='password',database='comparisdb')
cursor = cnx.cursor(buffered=True)
start = time.time()


session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=8))
session.mount("http://", HTTPAdapter(max_retries=8))
count = 0
def status(str):
    print(str)

def inc(): 
    global count 
    count += 1

pcount = 0
good_proxies = []

def clear_txt():
    f = open('/home/compscript/response.txt', 'r+')
    f.truncate(0) # need '0' when using r+
    f = open('/home/compscript/good2.txt', 'r+')
    f.truncate(0) # need '0' when using r+
   
def clear_states():
    f = open('/home/compscript/Aarau.txt', 'r+')
    f.truncate(0) # need '0' when using r+
    f = open('/home/compscript/Bern.txt', 'r+')
    f.truncate(0) # need '0' when using r+
    f = open('/home/compscript/Lucerne.txt', 'r+')
    f.truncate(0) # need '0' when using r+
    f = open('/home/compscript/Zug.txt', 'r+')
    f.truncate(0) # need '0' when using r+
    f = open('/home/compscript/Zurich.txt', 'r+')
    f.truncate(0) # need '0' when using r+

def proxies_list():
    headers={'User-Agent': ua.chrome}
    response = requests.get('https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/http.txt', headers=headers)
    with open("response.txt", "w") as f:
        f.write(response.text)
        f.close()

def proxies_arr():
    proxies_arr = []
    with open('response.txt', 'r') as reader:
        for line in reader.readlines():
            # print(line, end='')
            proxies_arr.append(line.strip())
    return proxies_arr

# #get the list of free proxies
# def getProxies():
#     r = requests.get('https://free-proxy-list.net/')
#     soup = BeautifulSoup(r.content, 'html.parser')
#     table = soup.find('tbody')
#     proxies = []
#     for row in table:
#         if row.find_all('td')[4].text =='elite proxy':
#             proxy = ':'.join([row.find_all('td')[0].text, row.find_all('td')[1].text])
#             proxies.append(proxy)
#         else:
#             pass
#     return proxies

def extract(proxy):
    global pcount
    headers={'User-Agent': ua.google}
    proxies={
            "http": proxy,
            "https": proxy,
        }
    # auth = HTTPProxyAuth("ahmdevnb", "d6n2kw7b9l03")
    while True:
        try:
            r = requests.get('https://www.comparis.ch/immobilien/result/list?requestobject=%7B%22DealType%22%3A20%2C%22SiteId%22%3A0%2C%22RootPropertyTypes%22%3A%5B%5D%2C%22PropertyTypes%22%3A%5B%5D%2C%22RoomsFrom%22%3Anull%2C%22RoomsTo%22%3Anull%2C%22FloorSearchType%22%3A0%2C%22LivingSpaceFrom%22%3Anull%2C%22LivingSpaceTo%22%3Anull%2C%22PriceFrom%22%3Anull%2C%22PriceTo%22%3Anull%2C%22ComparisPointsMin%22%3A0%2C%22AdAgeMax%22%3A0%2C%22AdAgeInHoursMax%22%3Anull%2C%22Keyword%22%3A%22%22%2C%22WithImagesOnly%22%3Anull%2C%22WithPointsOnly%22%3Anull%2C%22Radius%22%3A%2220%22%2C%22MinAvailableDate%22%3A%221753-01-01T00%3A00%3A00%22%2C%22MinChangeDate%22%3A%221753-01-01T00%3A00%3A00%22%2C%22LocationSearchString%22%3A%22Z%C3%BCrich%22%2C%22Sort%22%3A11%2C%22HasBalcony%22%3Afalse%2C%22HasTerrace%22%3Afalse%2C%22HasFireplace%22%3Afalse%2C%22HasDishwasher%22%3Afalse%2C%22HasWashingMachine%22%3Afalse%2C%22HasLift%22%3Afalse%2C%22HasParking%22%3Afalse%2C%22PetsAllowed%22%3Afalse%2C%22MinergieCertified%22%3Afalse%2C%22WheelchairAccessible%22%3Afalse%2C%22LowerLeftLatitude%22%3Anull%2C%22LowerLeftLongitude%22%3Anull%2C%22UpperRightLatitude%22%3Anull%2C%22UpperRightLongitude%22%3Anull%7D', proxies=proxies, headers=headers, timeout=2)
            if(r.status_code == 200):
                pcount = pcount + 1
                print(pcount, " ", proxy, " is working ", r.status_code)
                with open("good2.txt", "a") as myfile:
                    myfile.write(proxy)
                    myfile.write('\n')
                    myfile.close()
                good_proxies.append(proxy)
            break
        except requests.exceptions.ProxyError:
            print("Proxy Error Encountered: Reloading")
    
    return proxy





def getAllBuyProperties():
    # proxy = proxy + '/'
    status("GETTING RENT PROPERTIES....")
    ids = []
    time.sleep(1)
    
    with open('/home/compscript/urls.txt', 'r') as reader:
        for line in reader.readlines():
            while True:
                try:
                    url = line.strip()
                    response = requests.get(url, 
                        proxies={
                            "http": "http://d5b58097f4724f53b633fbdd6a5f82cc:@proxy.crawlera.com:8011/",
                            "https": "http://d5b58097f4724f53b633fbdd6a5f82cc:@proxy.crawlera.com:8011/",
                        },
                        verify='/home/compscript/zyte-smartproxy-ca.crt')
                    break
                except requests.exceptions.ProxyError:
                    print("Proxy Error Encountered: Reloading")

            soup = BeautifulSoup(response.text, 'lxml')
            div = soup.find('script',attrs = {'id':'__NEXT_DATA__'})
            j = json.loads(div.text)
            title = j["props"]["pageProps"]["pageTitle"]
            translated = GoogleTranslator(source='de', target='en').translate(text=title)
            state = translated.split()[-1]
            print(state)
            file = '/home/compscript/' + state + ".txt"
            ids = j["props"]["pageProps"]["initialResultData"]["adIds"]

            with open(file, "w") as  f:
                for line in ids:
                    f.write(str(line) + "\n") 
            print("successful written to the file ", file)
            f.close()
            

def getTimeRange(lines):
    arr = []
    count = math.ceil(lines / 24)
    timestamp = time.strftime('%H');
    hour = int(timestamp)
    arr = [count * hour,count * (hour + 1)]
    return arr



def readFile(file):
    with open(file, 'r') as f:
        arr = f.readlines()
        lines = len(arr)
        lines_range = getTimeRange(lines)
        print(lines_range)
        data = arr[lines_range[0]:lines_range[1]]
       
    f.close()
    return data

def saveData(file):
    # proxy = proxy + '/'
    cursor_count = 0
    section = "Buy"
    ids = readFile(file)
    print("SAVING DATA FOR ", Path(file).stem)
    for id in ids:
        new_id = str(id).strip()
        print(new_id)
        time.sleep(1)
        while True:
            try:
                response = requests.get(
                'https://www.comparis.ch/immobilien/marktplatz/details/show/' + new_id + '',
                    proxies={
                        "http": "http://d5b58097f4724f53b633fbdd6a5f82cc:@proxy.crawlera.com:8011/",
                        "https": "http://d5b58097f4724f53b633fbdd6a5f82cc:@proxy.crawlera.com:8011/",
                    },
                    verify='/home/compscript/zyte-smartproxy-ca.crt' 
                )
                if(int(response.status_code) == 503):
                    continue
                break
            except requests.exceptions.ProxyError:
                print("Proxy Error Encountered: Reloading")
        soup = BeautifulSoup(response.text, "lxml")
        div = soup.find('script',attrs = {'id':'__NEXT_DATA__'})
        print(response.status_code)
        j = json.loads(div.text)
        if "ad" in j["props"]["pageProps"]:
            pass
        else:
            continue
        phonenumber = ''
        if isinstance(j["props"]["pageProps"]["contactInformation"], dict):
            if isinstance(j["props"]["pageProps"]["contactInformation"]["VendorInformation"], dict):
                if "Phone" in j["props"]["pageProps"]["contactInformation"]["VendorInformation"]:
                    phonenumber = j["props"]["pageProps"]["contactInformation"]["VendorInformation"]["Phone"]
                else:
                    phonenumber = ""
            elif isinstance(j["props"]["pageProps"]["contactInformation"]["VisitationContactInformation"], dict):
                if "Phone" in j["props"]["pageProps"]["contactInformation"]["VisitationContactInformation"]:
                    phonenumber = j["props"]["pageProps"]["contactInformation"]["VisitationContactInformation"]["Phone"]
                else:
                    phonenumber = ""
        else:
            phonenumber = ""
        data = j["props"]["pageProps"]["ad"]
        state = Path(file).stem
        street = data["Address"]
        a = street.split()
        city = ','.join(str(x) for x in a[-2:])  
        maindata = data["MainData"]
        keys = list()
        vals = list()
        for x in maindata:
            keys.append(x["Key"])
            vals.append(x["Value"])
        pairs =  dict(zip(keys, vals))
        propertyType = pairs.get("PropertyType", "")
        numRooms = pairs.get("NumRooms","")
        floor = pairs.get("Floor","")
        livingSpace = pairs.get("LivingSpace","")
        constructionDate = pairs.get("YearOfConstruction","")
        availableDate = pairs.get("AvailableDate","")
        description = data["Title"]
        price = data["Price"]
        propertylink = new_id
        vals = (new_id,)
        cursor.execute('SELECT propertylink FROM properties WHERE propertylink = %s', vals)
        cnx.commit()
        newcount = cursor.rowcount
        if(newcount == 0):
            sql = 'INSERT INTO properties(section, state, street,city, propertyType, numRooms, floor, livingSpace,constructionDate, availableDate,price, description, phonenumber, propertylink) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            sql_vals =  (section, state, street,city, propertyType, numRooms, floor, livingSpace,constructionDate, availableDate,price, description, phonenumber, propertylink)

            cursor.execute(sql, sql_vals)
            cnx.commit()
            cursor_count = cursor_count + cursor.rowcount
            print("affected rows = " + str(cursor.rowcount))
        else:
            print("Already in Database")
    print("No of rows affected = ", cursor_count)

def statesInLists(state):
    proxies_arr = []
    file = '/home/compscript/' + state + '.txt'
    with open(file, 'r') as reader:
        for line in reader.readlines():
            proxies_arr.append(line.strip())
    return proxies_arr

def allPropertyLink(state):
    vals = (state,)
    cursor.execute('SELECT propertylink FROM properties WHERE state = %s', vals)
    cnx.commit()
    # print(cursor.rowcount)
    result = cursor.fetchone()
    row = [item[0] for item in cursor.fetchall()]
    return row
                
def checkNewProperties():
    states = ['Aarau', 'Bern', 'Lucerne', 'Zug', 'Zurich']
    for state in states:
        database = allPropertyLink(state)
        fetched_data = statesInLists(state)
        print(state + " database is " + str(len(set(database))))
        print(state + " new fetched data is " + str(len(set(fetched_data))))
        data = list(set(database).difference(set(fetched_data)))
        print("latest data of " + state)
        print("---------------------------")
        print(len(data))


# print(getTimeRange())
# print(save_proxies)
start = time.time()
clear_states()
getAllBuyProperties()
# clear_txt()

# proxies_list()
# proxylist = proxies_arr()
checkNewProperties()
# # print(test())
# with concurrent.futures.ThreadPoolExecutor() as executor:
#         executor.map(extract, proxylist)
# proxies = [*set(good_proxies)]
# print(len(proxies), " are working well")
# proxy = random.choice(proxies)
# hr = time.strftime('%H')
# clear_states()
# getAllBuyProperties(proxy)
# saveData("/home/compscript/Zurich.txt")
# saveData("/home/compscript/Lucerne.txt")
# saveData("/home/compscript/Aarau.txt")
# saveData("/home/compscript/Bern.txt")
# saveData("/home/compscript/Zug.txt")


cursor.close()
end = time.time()

print(end - start)
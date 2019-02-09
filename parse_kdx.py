# -*- coding: utf-8 -*-
import re,sys,time,base64,random,urllib,requests,multiprocessing,winsound
from idna import unicode
import urllib,threading
import db_broadbandx as DB
import db_customerx as DBC
from urllib import request,parse
from queue import Queue

def re_match(ptn, content):
    match_ptn = r'' + ptn + r'(=|:)"[^\"]*"'
    re_search = re.search(match_ptn, content)
    if re_search:
        return re_search.group()[len(ptn) + 2:-1]
    else:
        return ''


def parse_lt_kd(cust_id, cust_name, city='0312'):
    time.sleep(random.randint(1, 5))
    result = False
    kds = []

    id_num64 = str(base64.b64encode(cust_id.encode('utf-8')))[2:-1]
    cust_nm64 = str(base64.b64encode(cust_name.encode('utf-8')))[2:-1]
    query_type64 = str(base64.b64encode('1'.encode('utf-8')))[2:-1]
    city_code64 = str(base64.b64encode(city.encode('utf-8')))[2:-1]
    flag64 = str(base64.b64encode('list'.encode('utf-8')))[2:-1]
    ssid = 'AA0CBE9FB90164F9E0E55CF74FCC9338215'

    dwr_post2 = 'callCount=1' + \
                '\npage=/hb2/wap/wap_heb/kdcxnew/self-help-search2.html?' + \
                'QueryType=' + query_type64 + \
                '&QueryValue=' + id_num64 + \
                '&CityCode=' + city_code64 + \
                '&custName=' + cust_nm64 + \
                '&sysTag=' + \
                '\nhttpSessionId=' + \
                '\nscriptSessionId=' + ssid + \
                '\nc0-scriptName=Service' + \
                '\nc0-methodName=excute' + \
                '\nc0-id=0' + \
                '\nc0-param0=string:WB_QUERYXDSLSERVICEINFO' + \
                '\nc0-param1=boolean:false' + \
                '\nc0-e1=string:' + urllib.parse.quote(query_type64) + \
                '\nc0-e2=string:' + urllib.parse.quote(id_num64) + \
                '\nc0-e3=string:' + urllib.parse.quote(flag64) + \
                '\nc0-e4=string:' + urllib.parse.quote(city_code64) + \
                '\nc0-e5=string:' + urllib.parse.quote(cust_nm64) + \
                '\nc0-param2=Object_Object:{QueryType:reference:c0-e1, QueryValue:reference:c0-e2, FLAG:reference:c0-e3, CityCode:reference:c0-e4, custName:reference:c0-e5}' + \
                '\nbatchId=1'


    dwr_post_url = 'http://openapp.10010.com/hb2/wap/dwr/call/plaincall/Service.excute.dwr'
    req = request.Request(dwr_post_url, data=dwr_post2.encode('utf-8'))
    opener = request.build_opener()
    dwr2 = None
    try:
        response = opener.open(req, timeout=10)
        dwr2 = str(response.read().decode('utf-8'))
    except:
        print("             -->等待超时！")
        return result,kds

    else:
        result = True

    dwr2_strs = dwr2.split('distinctInfoRetrunList')
    kd = {}
    index = 1 # 宽带基础信息s序号
    appd = len(dwr2_strs)  # 宽带附加信息s序号
    serial = 1 # 宽带附加信息保存序号
    flag = True  # 头次处理宽带附加信息置True
    for dstr in dwr2_strs[1:]:
        kd = {}
        kd['NAME'] = cust_name
        kd['ID'] = cust_id
        kd['BroadbandRate'] = re_match('s' + str(index) + '.BroadbandRate', dstr).encode('utf-8').decode('unicode_escape')
        kd['NetTypeCode'] = re_match('s' + str(index) + '.NetTypeCode', dstr).encode('utf-8').decode('unicode_escape')
        kd['CustId'] = re_match('s' + str(index) + '.CustId', dstr)
        kd['ProductId'] = re_match('s' + str(index) + '.ProductId', dstr)
        kd['RealFee'] = re_match('s' + str(index) + '.RealFee', dstr)
        kd['DepositMoney'] = re_match('s' + str(index) + '.DepositMoney', dstr)
        kd['OweFee'] = re_match('s' + str(index) + '.OweFee', dstr)
        kd['SerialNumber'] = re_match('s' + str(index) + '.SerialNumber', dstr)  # 宽带号
        kd['InternetAccount'] = re_match('s' + str(index) + '.InternetAccount', dstr)
        kd['OpenDate'] = re_match('s' + str(index) + '.OpenDate', dstr)
        kd['ProductName'] = re_match('s' + str(index) + '.ProductName', dstr).encode('utf-8').decode('unicode_escape')
        kd['UserState'] = re_match('s' + str(index) + '.UserState', dstr).encode('utf-8').decode('unicode_escape')
        kd['BroadbandAddress'] = re_match('s' + str(index) + '.BroadbandAddress', dstr).encode('utf-8').decode('unicode_escape')
        flag = True
        while re_match('s'+str(appd)+'.DiscntName', dstr) or flag:
            if re_match('s'+str(appd)+'.DiscntName', dstr):
                kd['StartDate' + str(serial)] = re_match('s' + str(appd) + '.StartDate', dstr)
                kd['DiscntCode' + str(serial)] = re_match('s' + str(appd) + '.DiscntCode', dstr)
                kd['EndDate' + str(serial)] = re_match('s' + str(appd) + '.endDate', dstr)
                kd['DiscntName' + str(serial)] = re_match('s' + str(appd) + '.DiscntName', dstr).encode('utf-8').decode('unicode_escape')
                serial += 1
            flag = False
            appd += 1
        kds.append(kd)
        serial = 1
        index += 1
    # print(dwr2)
    # print(kds)
    return result,kds

def save_a_kd(kds):
    for kd in kds:
        DB.insert_a_row(**kd)

def parse_noproxy_AT(fetchrows=1000):
    custs = list(DBC.fetch_rows_to_parse_tmp1(fetchrows))
    print("custs="+str(custs))
    for cust in custs:
        cust_name, cust_id = cust[0], cust[1]
        print("Parsing now! --> name:" + cust_name + ",id:" + cust_id)
        kds = parse_lt_kd(cust_id, cust_name)
        if not kds:
            print("             -->未查询到宽带信息！")
            DBC.write_a_log(cust_name, cust_id, 'NO')
        else:
            save_a_kd(kds)
            DBC.write_a_log(cust_name, cust_id, 'YES')

class KD_Parser(threading.Thread):
    def __init__(self,cust_queue,*args,**kwargs):
        super(KD_Parser,self).__init__(*args,**kwargs)
        self.cust_queue = cust_queue

    def run(self):
        while True:
            if self.cust_queue.empty():
                for x in range(3):
                    winsound.Beep(2600, 600)
                break
            cust = self.cust_queue.get()
            cust_name, cust_id = cust[0], cust[1]
            result,kds = False,None
            while not result:
                print("Parsing now! --> name:" + cust_name + ",id:" + cust_id)
                result, kds = parse_lt_kd(cust_id, cust_name)
            if not kds:
                print("             -->未查询到宽带信息！")
                DBC.write_a_log(cust_name, cust_id, 'NO')
            else:
                save_a_kd(kds)
                DBC.write_a_log(cust_name, cust_id, 'YES')
            DBC.delete_row(cust_name, cust_id)

def parse_noproxy_multithread_AT(fetchrows=1000,threads=3):
    for x in range(5):
        winsound.Beep(2600, 1100-x*200)
    custs = list(DBC.fetch_rows_to_parse(fetchrows))
    if not custs:
        print("fetch no rows!")
        return
    fetchrows = len(custs)
    custs_q = Queue(fetchrows)
    for x in range(fetchrows):
        custs_q.put(custs[x])
    for x in range(threads):
        t = KD_Parser(custs_q)
        t.start()
        time.sleep(3)


def testcase1_parse_noproxy_AT():
    cust_id = '130624198602283415'
    cust_name = '齐亚飞'
    print("Parsing now! --> name:"+cust_name+",id:"+cust_id)
    kds = parse_lt_kd(cust_id,cust_name)
    if not kds:
        print("             -->未查询到宽带信息！")
        DBC.write_a_log(cust_name, cust_id, 'NO')
    else:
        save_a_kd(kds)
        DBC.write_a_log(cust_name, cust_id, 'YES')



if __name__ == '__main__':
    # parse_noproxy_AT(10)
    parse_noproxy_multithread_AT(fetchrows=20000,threads=50)

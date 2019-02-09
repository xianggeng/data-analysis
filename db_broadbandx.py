import pymysql
# test in here one!@xxx

def insert_a_row(**kargs):
    name,idnum,serialnum,prdname,opendate,nettype,rate,realfee,owefee,depositmoney,userstat,addr,discntname1,startdate1,enddate1,discntname2,startdate2,enddate2,discntname3,startdate3,enddate3,discntname4,startdate4,enddate4,discntname5,startdate5,enddate5="","","","",None,"","","","","","","","",None,None,"",None,None,"",None,None,"",None,None,"",None,None
    if "NAME" in kargs.keys():
        name = kargs["NAME"]
    if "ID" in kargs.keys():
        idnum = kargs["ID"]
    if "SerialNumber" in kargs.keys():
        serialnum = kargs["SerialNumber"]
    if "ProductName" in kargs.keys():
        prdname = kargs["ProductName"]
    if "OpenDate" in kargs.keys():
        opendate = kargs["OpenDate"]
    if "NetTypeCode" in kargs.keys():
        nettype = kargs["NetTypeCode"]
    if "BroadbandRate" in kargs.keys():
        rate = kargs["BroadbandRate"]
    if "RealFee" in kargs.keys():
        realfee = kargs["RealFee"]
    if "OweFee" in kargs.keys():
        owefee = kargs["OweFee"]
    if "DepositMoney" in kargs.keys():
        depositmoney = kargs["DepositMoney"]
    if "UserState" in kargs.keys():
        userstat = kargs["UserState"]
    if "BroadbandAddress" in kargs.keys():
        addr = kargs["BroadbandAddress"]
    if "DiscntName1" in kargs.keys():
        discntname1 = kargs["DiscntName1"]
    if "StartDate1" in kargs.keys():
        startdate1 = kargs["StartDate1"]
    if "EndDate1" in kargs.keys():
        enddate1 = kargs["EndDate1"]
    if "DiscntName2" in kargs.keys():
        discntname2 = kargs["DiscntName2"]
    if "StartDate2" in kargs.keys():
        startdate2 = kargs["StartDate2"]
    if "EndDate2" in kargs.keys():
        enddate2 = kargs["EndDate2"]
    if "DiscntName3" in kargs.keys():
        discntname3 = kargs["DiscntName3"]
    if "StartDate3" in kargs.keys():
        startdate3 = kargs["StartDate3"]
    if "EndDate3" in kargs.keys():
        enddate3 = kargs["EndDate3"]
    if "DiscntName4" in kargs.keys():
        discntname4 = kargs["DiscntName4"]
    if "StartDate4" in kargs.keys():
        startdate4 = kargs["StartDate4"]
    if "EndDate4" in kargs.keys():
        enddate4 = kargs["EndDate4"]
    if "DiscntName5" in kargs.keys():
        discntname5 = kargs["DiscntName5"]
    if "StartDate5" in kargs.keys():
        startdate5 = kargs["StartDate5"]
    if "EndDate5" in kargs.keys():
        enddate5 = kargs["EndDate5"]
    kd = (name,idnum,serialnum,prdname,opendate,nettype,rate,realfee,owefee,depositmoney,userstat,addr,discntname1,startdate1,enddate1,discntname2,startdate2,enddate2,discntname3,startdate3,enddate3,discntname4,startdate4,enddate4,discntname5,startdate5,enddate5)
    db = pymysql.connect(
        host='localhost',
        user='root',
        password='Gh000065',
        database='spider',
        port=3306
    )
    cursor = db.cursor()
    sql = """
        insert into broadbandx(name,idnum,serialnum,prdname,opendate,nettype,rate,realfee,owefee,depositmoney,userstat,addr,discntname1,startdate1,enddate1,discntname2,startdate2,enddate2,discntname3,startdate3,enddate3,discntname4,startdate4,enddate4,discntname5,startdate5,enddate5) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    cursor.execute(sql, kd)
    db.commit()
    print("Success insert a row! -->" + str(kd))
    db.close()

def is_row_exist(serialnum):
    flag = False
    db = pymysql.connect(
        host='localhost',
        user='root',
        password='Gh000065',
        database='spider',
        port=3306
    )
    cursor = db.cursor()
    sql = "select * from broadbandx where serialnum='%s'" % serialnum
    cursor.execute(sql)
    results = cursor.fetchall()
    if results:
        flag = True
    db.close()
    print("test in here two!@xxx")
    return flag

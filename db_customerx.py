import pymysql

def delete_row(username,idnum):
    db = pymysql.connect(
        host='localhost',
        user='root',
        password='Gh000065',
        database='spider',
        port=3306
    )
    cursor = db.cursor()
    sql = "delete from customerx where username='%s' and idnum='%s'" % (username,idnum)
    cursor.execute(sql)
    db.commit()
    db.close()

def write_a_log(username,idnum,flag):
    if is_row_exist(username,idnum):
        return
    else:
        db = pymysql.connect(
            host='localhost',
            user='root',
            password='Gh000065',
            database='spider',
            port=3306
        )
        cursor = db.cursor()
        sql = """
            insert into customer_logx(username,idnum,flag) values ('%s','%s','%s')
            """ % (username, idnum, flag)
        cursor.execute(sql)
        db.commit()
        db.close()

def is_row_exist(username,idnum):
    flag = False
    db = pymysql.connect(
        host='localhost',
        user='root',
        password='Gh000065',
        database='spider',
        port=3306
    )
    cursor = db.cursor()
    sql = "select * from customer_logx where username='%s' and idnum='%s'" % (username,idnum)
    cursor.execute(sql)
    results = cursor.fetchall()
    if results:
        flag = True
    db.close()
    return flag

def fetch_rows_to_parse(size=100):
    db = pymysql.connect(
        host='localhost',
        user='root',
        password='Gh000065',
        database='spider',
        port=3306
    )
    cursor = db.cursor()
    sql = """
        select distinct a.* from customerx a
        left join broadbandx b on a.idnum=b.idnum and a.username=b.name
        left join customer_logx c on a.idnum=c.idnum and a.username=c.username
        where b.idnum is null and c.idnum is null
        limit 
    """ + str(size)
    cursor.execute(sql)
    rows = cursor.fetchall()
    db.commit()
    db.close()
    return rows

def fetch_rows_to_parse_tmp1(size=100):
    db = pymysql.connect(
        host='localhost',
        user='root',
        password='Gh000065',
        database='spider',
        port=3306
    )
    cursor = db.cursor()
    sql = """
        select distinct b.name,b.idnum from broadband b 
        left join customer_logx c on b.name=c.username and b.idnum=c.idnum
        where c.username is null
        limit 
    """ + str(size)
    cursor.execute(sql)
    rows = cursor.fetchall()
    db.commit()
    db.close()
    return rows
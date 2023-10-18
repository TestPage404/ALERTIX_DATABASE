import psycopg2
import os
import subprocess
import sys
from datetime import date
import config
import re



host = config.host
db_user = config.db_user
db_pass = config.db_pass
db_name = config.db_name
alertix_pw = config.alertix_pw
alertix_ip = config.alertix_ip
tempfile_alertix = config.tempfile_alertix
tempfile_rdg = config.tempfile_rdg
path = config.path

####### ####### Скачаиваем все файлы с сервера Alertix ####### #######
os.system(f'cmd /c pscp -pw {alertix_pw} -r alertix@{alertix_ip}:/mnt/ssd1/MSK_logs/{date.today()}* d:/MSK_logs')
####### ####### ####### ####### #######
print("Файлы скачаны.")

####### ####### Собираем все файлы в один ####### #######

ls = [i for i in os.listdir(path) if i.endswith('')]
ls.sort()
with open(tempfile_alertix,'w') as f:
    for j in ls:
        s = open(path + j).read()
        f.write(re.sub('\.[0-9]+Z', '', s))
        os.remove(path + j)
####### ####### ####### ####### #######
print("Файлы собраны в один.")
####### ####### Заполняем БД находяющуюся на Докере ####### #######


connection = psycopg2.connect(host=host, user=db_user, password=db_pass, database=db_name)
connection.autocommit = True


def KeePassDict():
    with open(tempfile_alertix) as f:
        keepass_list = []
        for i in f:
            i = tuple(i.strip().replace("\"", "").split(','))
            keepass_list.append(i)
        return  keepass_list


def KeepassToDB(values):
    print("Начинаю заполнение БД.")
    with connection.cursor() as cur:
        #cur.execute("""DROP TABLE IF EXISTS alertix""")
        cur.execute("""CREATE TABLE IF NOT EXISTS alertix(
            time timestamp,
            ip text,
            username text,
            dc text
        )""")
        cur.executemany("""INSERT INTO alertix(dc, ip, username, time) VALUES(%s,%s,%s,%s)""", values)
        #cur.execute("""alter table alertix drop column dc""")




if __name__ == '__main__':
    values = set(KeePassDict())
    KeepassToDB(values)
    print("БД Заполнена.")
####### ####### ####### ####### #######

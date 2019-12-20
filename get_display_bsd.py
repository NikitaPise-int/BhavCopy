import cherrypy
import os.path
import redis
import pickle
import json
import csv
import sys
import io
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import datetime
from datetime import date,timedelta
import requests
from io import BytesIO
from zipfile import ZipFile
env = Environment(loader=FileSystemLoader('templates'))


class getDisplayBSD:
    def get_csv(self,url,filename):
        print("get_csv......................................................................................................")
        r = requests.get(url+filename, stream =True)
        z = ZipFile(io.BytesIO(r.content))
        z.extractall()
        return True

        # columns = (0, 1,2,3,4) if len(sys.argv) < 4 else (int(x) for x in sys.argv[2:4])
        # data = self.read_csv_data(string_data, *columns)
        # return data

    def read_csv_data(self,csv_file, sc_code, sc_name, Open, high, low, close):
        with open(csv_file, encoding='utf-8') as csvf:
            csv_data = csv.reader(csvf)
            next(csv_data, None)
            return [(r[sc_code], r[sc_name], r[Open], r[high], r[low], r[close]) for r in csv_data]

    def store_data(self,conn, data):
        
        for i in data:
            conn.setnx(i[0], i[1])
        return data

    def index(self):
        
        conn = redis.StrictRedis(host='ec2-34-233-1-51.compute-1.amazonaws.com',user='h', password='redis://h:p093ab09f6072b0e5f5d491cea35851e7edc9a067b05953e8a3962470f3ef14f3@ec2-34-233-1-51.compute-1.amazonaws.com:21729', port=21729)
        outdata = []
        columns = (0, 1,2,3,4,5) if len(sys.argv) < 4 else (int(x) for x in sys.argv[2:5])
        dt = pd.date_range(start=datetime.date.today()- timedelta(days = 2), periods=10, freq='B')

        if datetime.date.today() in dt:
            print (datetime.date.today().strftime("%d%m%y"))
            latestDate = datetime.date.today().strftime("%d%m%y")
        else:
            print(dt.min().strftime("%d%m%y"))
            latestDate = dt.min().strftime("%d%m%y")
        url = "https://www.bseindia.com/download/BhavCopy/Equity/"
        #EQ131219_CSV.ZIP
        filename = "EQ"+latestDate+"_CSV.ZIP"
        file = "EQ"+latestDate+".CSV"
        fileGenerate = self.get_csv(url,filename)
        if fileGenerate:
            columns = (0,1,4,5,6,7) if len(sys.argv) < 4 else (int(x) for x in sys.argv[2:4])
            data = self.read_csv_data(file,*columns)
        print(data)
        tmpl = env.get_template('index.html')
        return tmpl.render(list=json.dumps(self.store_data(conn, data)))

    index.exposed = True

configfile = os.path.join(os.path.dirname(__file__),'server.conf')
cherrypy.quickstart(getDisplayBSD(),config = configfile)


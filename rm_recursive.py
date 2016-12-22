#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import json
import string
import urllib
import datetime
import random
import threading
import time
import logging
import datetime
from time import strftime, localtime
from datetime import date
from datetime import timedelta
from lib import threadpool
from lib import CosClient
from lib import DelFileRequest
from lib import DelFolderRequest
from lib import ListFolderRequest
from time import sleep
from logging.handlers import RotatingFileHandler

MAX_RETRY_TIMES = 3
LOG_SAVE_EVERY_NUM = 1024
ONE_TASK_DEL_FILE_NUMS = 5
CONFIG_FILE_PATH = "./conf/config.json"
BUCKET_PATH_LIST_PATH = "./conf/bucketlist.txt"

def loginit():
    global config
    if (config.log_file_name == ""):
        return
    log_level = logging.ERROR
    if config.log_level == 0:
        log_level = logging.DEBUG
    if config.log_level == 1:
        log_level = logging.INFO
    if config.log_level == 2:
        log_level = logging.WARNING

	#定义一个RotatingFileHandler，最多备份5个日志文件，每个日志文件最大20M
    logger = logging.getLogger("")
    Rthandler = RotatingFileHandler(config.log_file_name, maxBytes= 20*1024*1024,backupCount=5)
    Rthandler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    Rthandler.setFormatter(formatter)
    logger.addHandler(Rthandler)
    logger.setLevel(log_level)
    return logger

#日期相关操作
class Dateop():
    @staticmethod
    def isValidDate(str):
        try:
            time.strptime(str, "%Y""%m""%d")
            return True
        except:
            return False

    @staticmethod
    def getdaystr(n=0):
        dt = date.today()-timedelta(days=n)
        tt = dt.timetuple()
        daystr = strftime("%Y""%m""%d",tt)
        return daystr

    @staticmethod
    def cmpDateAgo(t1,t2):
        if (Dateop.isValidDate(t1)==False or Dateop.isValidDate(t2)==False):
            return False
        if (int(t1) <= int (t2)):
            return True
        return False

    @staticmethod
    def isNeedDeleteDir(dirname, n=0):
        if (len(dirname) != 8):
            return False
        if Dateop.isValidDate(dirname) == False:
            return False
        d2 = Dateop.getdaystr(n);
        if Dateop.cmpDateAgo(dirname, d2):
            return True
        return False
#删除文件统计
class FileStat():
    global cos_log
    def __init__(self):
        self.delfilesuccnum = 0
        self.deldirsuccnum = 0
        self.delfilefailnum = 0
        self.deldirfailnum = 0
        self.lock = threading.Lock()

    def addDelFileFailNum(self,num=1):
        self.lock.acquire(1)
        self.delfilefailnum += num
        self.lock.release()
    def addDelDirFailNum(self,num=1):
        self.lock.acquire(1)
        self.deldirfailnum += num
        self.lock.release()
    def addDelDirSuccNum(self, num=1):
        self.lock.acquire(1)
        self.deldirsuccnum += num
        self.lock.release()
    def addDelFileSuccNum(self, num=1):
        self.lock.acquire(1)
        self.delfilesuccnum += num
        self.lock.release()
    def printStat(self):
        msg ="".join(["delfilesuccnum=",str(self.delfilesuccnum),
                ",delfilefailnum=",str(self.delfilefailnum),
                ",deldirsuccnum=",str(self.deldirsuccnum),
                ",deldirfailnum=",str(self.deldirfailnum)])
        print(msg) 
    def logStat(self):
        curtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log = ''.join(["delfilenum=",str(self.delfilesuccnum),
            ",deldirnum=",str(self.deldirsuccnum),",delfilefailnum=",
            str(self.delfilefailnum),",deldirfailnum=",str(self.deldirfailnum)])
        cos_log.info(log)
        print(log)

#执行时间统计
class TimeStat(object):
    global cos_log
    def __init__(self):
		self.start()
    def start(self):
        self.start = datetime.datetime.now()
        self.t1 = time.time()
        msg = "delete task started  ..........."
        print(msg)
        cos_log.info(msg)
    def end(self):
        self.end = datetime.datetime.now()
        self.t2 = time.time()
        msg = "delete task ended\n\nrm task finished,\ntimecost:"+str(self.t2-self.t1) + " (s)"
        print(msg)
        cos_log.info(msg)

#删除文件列表中的文件
def delfiles(cos_client, bucket, filelist):
    for f in filelist:
        delfile(cos_client, bucket, f)

#文件夹删除
def delfolder(cos_client, bucket, folder):
    global stat
    global cos_log
    if not folder:
        return 0
    delfolderreq = DelFolderRequest(bucket, folder)
    retry = 0
    while (retry < MAX_RETRY_TIMES):
        ret = cos_client.del_folder(delfolderreq)
        msg = "delfolder fail, bucket="+bucket+",folder="+folder+ret['message']
        if (ret['code'] == 0):
            break
        elif (ret['code'] == -166):
            cos_log.warning(msg)
            break
        #操作太频繁,频控
        elif (ret['code'] == -71):
            sleep(random.randint(1,5))
            cos_log.warning(msg)
            print(msg)
            retry += 1
            continue
        #文件夹非空
        elif (ret['code'] == -173):
            break
        else:
            cos_log.warning(msg)
            retry += 1
    if (ret['code'] != 0 and  ret['code'] != -166):
        stat.addDelDirFailNum()
        cos_log.error("delfolder fail, bucket="+bucket+",folder="+folder+ret['message'])
        return ret['code']
    if (ret['code'] == 0):
        stat.addDelDirSuccNum()
        msg = "delfolder success, bucket="+bucket+",folder="+folder
        cos_log.info(msg)
        print(msg)
    return 0

#文件删除
def delfile(cos_client, bucket, filepath):
    global stat
    global cos_log
    delfilereq = DelFileRequest(bucket, filepath)
    retry = 0
    while (retry < MAX_RETRY_TIMES):
        ret = cos_client.del_file(delfilereq)
        msg = "delfile fail bucket="+bucket+",file="+filepath+ret['message']
        if (ret['code'] == 0):
            break
        #文件不存在
        elif (ret['code'] == -166):
            cos_log.warning(msg)
            break
        #单目录写操作过快
        elif (ret['code'] == -143):
            sleep(random.randint(1,5))
            cos_log.warning(msg)
            print(msg)
            retry += 1
            continue
        #操作太频繁,频控
        elif (ret['code'] == -71):
            sleep(random.randint(1,5))
            cos_log.warning(msg)
            print(msg)
            retry += 1
            continue
        else:
            cos_log.warning(msg)
            retry += 1
            continue
    if (ret['code'] != 0 and  ret['code'] != -166):
        stat.addDelFileFailNum()
        cos_log.error("delfile fail, bucket="+bucket+",file="+filepath+ret['message'])
        return ret['code']
    if (ret['code'] == 0):
        stat.addDelFileSuccNum()
        msg = "delfile success, bucket="+bucket+",file="+filepath
        cos_log.info(msg)
        print(msg)
    return 0

#递归文件夹进行文件删除
def delete_r(cos_client, bucket, path, thread_pool_file):
    global stat
    global config
    global cos_log
    cos_log.debug("delete_r bucket:"+bucket+",path:"+path)
    context = ""
    #递归文件夹
    while True:
        listfolderreq = ListFolderRequest(bucket, path,199,u'eListDirOnly')
        retry = 0
        while (retry < MAX_RETRY_TIMES):
            listret = cos_client.list_folder(listfolderreq)
            if listret['code'] != 0 :
                retry += 1
                sleep(random.randint(1,5))
                continue
            else:
                break
        if (listret['code'] != 0):
            cos_log.error("delete_r: list folder fail:"+path +",return msg:"+ listret['message'])
            return listret['code']
        if (len(listret['data']['infos']) == 0):
            break;
        for info in listret['data']['infos']:
            fullname = path + info['name'] + u'/'
            #当指示为False的时候不用比较,否则该标识一直递归传递下去
            ret = delete_r(cos_client, bucket, fullname, thread_pool_file)
            #当设置"删除失败不退出标志时",程序继续进行删除操作
            if (ret != 0 and config.delete_fail_exist == 1):
                return ret
        if (listret['data']['has_more'] == True):
            context = listret['data']['context']
            continue
        else:
            break
    #本文件夹不删则不用list本文件夹的文件列表
    while True:
        listfilereq = ListFolderRequest(bucket, path,199,u'eListFileOnly')
        retry = 0
        while (retry < MAX_RETRY_TIMES):
            listret = cos_client.list_folder(listfilereq)
            if listret['code'] != 0 :
                retry += 1
                sleep(random.randint(1,5))
                continue
            else:
                break
        if (listret['code'] != 0):
            cos_log.error("delete_r: list file fail:"+path +",return msg:"+ listret['message'])
            return listret['code']
        
        if (len(listret['data']['infos']) == 0):
            break;

        filelist = []
        for info in listret['data']['infos']:
            fullname = path + info['name']
            filelist.append(fullname)
            if (len(filelist) == config.one_task_del_file_num):
                args = [cos_client, bucket, filelist]
                args_tuple = (args,None)
                args_list = [args_tuple]
                requests = threadpool.makeRequests(delfiles, args_list)
                for req in requests:
                    thread_pool_file.putRequest(req)
                filelist = []
                continue
        if (len(filelist) > 0):
            args = [cos_client, bucket, filelist]
            args_tuple = (args,None)
            args_list = [args_tuple]
            requests = threadpool.makeRequests(delfiles, args_list)
            for req in requests:
                thread_pool_file.putRequest(req)
            filelist = []

        cos_log.debug("delete_r thread pool file waiting\n")
        thread_pool_file.wait()
        cos_log.debug("delete_r thread pool file waiting end\n")

        if (listret['data']['has_more'] == True):
            continue
        else:    
            break
    #删除本文件夹(上面已经删除子文件夹及子文件)
    if (path != u"/"):
        ret = delfolder(cos_client,bucket, path)
        #当设置"删除失败不退出标志时",程序继续进行删除操作
        if (ret != 0 and config.delete_fail_exist == 1):
            return ret
    stat.logStat()
    return 0

#解析配置文件
class Config:
    def __init__(self, configfile):
        fp = open(configfile)
        try:
            config = json.loads(fp.read())
        finally:
            fp.close()
        self.appid = config['appid']
        self.secret_id = config['secret_id'].decode("utf8")
        self.secret_key = config['secret_key'].decode("utf8")
        self.log_level = config['log_level']
        self.log_file_name = config['log_file_name']
        self.dir_thread_num = config['dir_thread_num']
        self.file_thread_num = config['file_thread_num']
        self.one_task_del_file_num = ONE_TASK_DEL_FILE_NUMS
        self.delete_fail_exist = config['delete_folder_fail_exist']

#解析待删除的bucket和文件夹
class BucketDirList:
    def __init__(self, fname):
        fp = open(fname)
        try:
            self.bucketDirList = fp.readlines()
        finally:
            fp.close()
    
    def getBucketName(self,bp):
        bucket_dir = bp.strip().split(',')
        if (len(bucket_dir) != 2):
            return ""
        bucket = bucket_dir[0].strip().decode("utf8")
        return bucket
    def getPath(self,bp):
        bucket_dir = bp.strip().split(',')
        if (len(bucket_dir) != 2):
            return ""
        path = bucket_dir[1].strip().decode("utf8")
        return path

def main():
    global thread_pool
    global config
    global cos_log
    bucketdirList = BucketDirList(BUCKET_PATH_LIST_PATH)
    cos_client = CosClient(config.appid, config.secret_id, config.secret_key)
    thread_pool_dir = threadpool.ThreadPool(config.dir_thread_num)
    for var in bucketdirList.bucketDirList:
        cos_log.debug(var)
        bucket = bucketdirList.getBucketName(var)
        path = bucketdirList.getPath(var)
        if (bucket == "" or path == ""):
            continue
        thread_pool_file = threadpool.ThreadPool(config.file_thread_num)
        cos_log.debug("bucket:"+bucket +",path:"+path)
        args = [cos_client,bucket, path, thread_pool_file]
        args_tuple = (args, None)
        args_list = [args_tuple]
        requests = threadpool.makeRequests(delete_r, args_list)
        for req in requests:
            thread_pool_dir.putRequest(req)
    cos_log.debug("thread_pool_dir waiting.....\n")
    thread_pool_dir.wait()
    thread_pool_dir.dismissWorkers(config.dir_thread_num, True)
    cos_log.debug("thread_pool_dir wait end.....\n")

if __name__ == '__main__':
    global cos_log
    config = Config(CONFIG_FILE_PATH)
    cos_log = loginit()
    stat = FileStat()
    timestat = TimeStat()
    main()
    timestat.end()
    stat.logStat()

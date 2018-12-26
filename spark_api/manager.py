# ---------------pyspark-----------------
from pyspark.sql import SparkSession  #--
from pyspark import SparkConf         #--
from pyspark.sql import functions     #--  
from pyspark.sql import HiveContext   #--
# ---------------------------------------
from core.address import address_extra_product as Address
from core.identity import id_check_product as Idcard
from core.phone import phone_find_product as Phone
from core.dataprocess import data_product as data_etl
# os.environ['PYSPARK_PYTHON']='/opt/miniconda3/bin/python3.6' 


class SparkManager:

    def __init__(self):

        self.conf = SparkConf().setAll([('spark.executor.memory','2g'),
        ('spark.executor.instances','18'),
        ('spark.executor.cores','2'),
        ('spark.cores.max','36')
        #('spark.dynamicAllocation.enabled','true'),
        #('spark.dynamicAllocation.initialExecutors','18'),
        #('spark.shuffle.service.enabled','true'),
        #('spark.dynamicAllocation.minExecutors','10')
        ])
         
        self.spark = SparkSession.builder \
        .appName('DataEngine') \
        .master('yarn') \
        .config(conf=self.conf)\
        .enableHiveSupport() \
        .getOrCreate()

        # self.sc = self.spark.sparkContext
        # self.hc = HiveContext(self.sc)

        print("spark.........初始化完毕")
    
    def sparkCreator(self):
        
        # HiveUDF RegisterFunction
        def hiveUDFregister(hc):
            ######## Phone Register ########
        #     hc.registerFunction('PHONE', Phone.getInfo)
            hc.registerFunction('PHONE_PROVINCE', Phone.getProv)
            hc.registerFunction('PHONE_CITY', Phone.getCity)
            hc.registerFunction('PHONE_ZIPCODE', Phone.getZip)
            hc.registerFunction('PHONE_AREACODE', Phone.getArea)
            hc.registerFunction('PHONE_OPERATOR', Phone.getOper)

            ######## Idcard Register ########
        #     hc.registerFunction('IDCARD', Idcard.getInfo)
            hc.registerFunction('IDCARD_STATUS', Idcard.getStatus)
            hc.registerFunction('IDCARD_AREA', Idcard.getArea)
            hc.registerFunction('IDCARD_BIRTHDAY', Idcard.getBirth)
            hc.registerFunction('IDCARD_AGE', Idcard.getAge)
            hc.registerFunction('IDCARD_SEX', Idcard.getSex)

            ######## Address Register ########
        #     hc.registerFunction('ADDRESS', Address.getInfo)
            hc.registerFunction('ADDRESS_PROVINCE', Address.getProv)
            hc.registerFunction('ADDRESS_CITY', Address.getCity)
            hc.registerFunction('ADDRESS_AREA', Address.getArea)
            hc.registerFunction('ADDRESS_COORDINATE', Address.getCoor)
            
            ######## NewFunctions Register ########
            hc.registerFunction('HIDE', data_etl.transInfo)
            hc.registerFunction('FILL', data_etl.fillInfo)

        def addfile(sc):
            sc.addFile('/Users/junwen/DataServer/src/phone.dat')
            sc.addFile('/Users/junwen/DataServer/src/pcc_2017.json')
            sc.addFile('/Users/junwen/DataServer/src/address_dict.txt')
            sc.addFile('/Users/junwen/DataServer/src/dict.txt')
            sc.addPyFile('/Users/junwen/DataServer/src/DataServer.zip')

        spark = self.spark
        # newSparkSession = spark.newSession()
        sc = spark.sparkContext
        hc = HiveContext(sc)
        addfile(sc)
        hiveUDFregister(hc)
        application_ID = sc.applicationId
        print(application_ID)
        return spark,sc,hc,application_ID

    @staticmethod
    def runSparkSQL(sqlInput,sc,hc,jobGroupID=None,sync=None):
        sc.setJobGroup(jobGroupID,"some-description")
        result = hc.sql(sqlInput)
        result.show()

        if sync:
            resultList = [[row[key] for key in result.columns] for row in result.collect()]
            return resultList
    
    @staticmethod
    def spark_listener(sc,jobGroupID=None):
        big_list = []
        list_info = []
        status = sc.statusTracker()
        ids = status.getJobIdsForGroup(jobGroupID)
        for i in ids:
            job = status.getJobInfo(i)
            for sid in job.stageIds:
                info = status.getStageInfo(sid)
                if info:
                    list_info = [i,job.status,sid,info.numTasks,info.numActiveTasks,info.numCompletedTasks]
                    big_list.append(list_info)
        return big_list
    
    @staticmethod
    def scConfig(sc):
        sc.setLogLevel()
    

    @staticmethod
    def stop_job(sc,jobID):
        sc.cancelJobGroup(jobID)

    @staticmethod
    def killSession(sc):
        sc.stop()
    
# test = SparkManager()
# spark,mysc,myhc,appid = test.sparkCreator()
# mysc.stop()




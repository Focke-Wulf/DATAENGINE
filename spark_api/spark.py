# ---------------pyspark-----------------
from pyspark.sql import SparkSession  #--
from pyspark import SparkConf         #--
from pyspark.sql import functions     #--  
from pyspark.sql import HiveContext   #--
# ---------------------------------------

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
        spark = self.spark
        sc = spark.sparkContext
        hc = HiveContext(sc)
        application_ID = sc.applicationId
        return spark,sc,hc,application_ID
        
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


test = SparkManager()
spark,sc,hc,appid = test.sparkCreator()
print(appid)
sc.stop()
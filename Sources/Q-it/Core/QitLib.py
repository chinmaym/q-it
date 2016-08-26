import os
import redis
import json
from SiriusBase.Core.ExtBaseObject import ExtBaseObject
from SiriusBase.Core.ConfigManager import ConfigManager
from SiriusBase.Utils.PathUtils import GetAbsolutePath
from pkg_resources import resource_filename
from AutoCITIQueue.Core.Task import Task
import logging.config
import uuid



class AutoCITIQueue(ExtBaseObject):
    DEFAULT_USER_CONF_PATH = '~/.AutoCITIQueue/AutoCITIQueue.conf'
    DEFAULT_AUTOCITI_QUEUE_HASH_MAP = 'AutoCITIQueue' 
    DEFAULT_AUTOCITI_JOB_TRACKER_HASH_MAP = 'AutoCITIJobTracker'
    DEFAULT_PRIORITY_SEPERATOR = ';'
    DEFAULT_PRIORITY_LEVELS = 'High,Medium;Low'
    DEFAULT_QUEUE_NAME_TEMPLATE = '%s_%s'
    
    def __init__(self):
        ExtBaseObject.__init__(self)
        self._rdb = None
        self._connected = False
        self._AutoCITIQueueHashMap = None
        self._AutoCITIJobTracker = None 
        self._PriorityLevels = None
        self._QueueNameTemplate = None
        self._AutoCITIQueues = dict()
        self._AppSetup()
        self._ProcessConfiguration()
        self._ConnectRedis()
        self._LoadQueues()
        
    def _ProcessConfiguration(self):
        
        autoCITIQueueHashMap = self._ConfigManager.GetSettingValue('RedisHashMaps','AutoCITIQueueHashMap')
        if not autoCITIQueueHashMap:
            autoCITIQueueHashMap = AutoCITIQueue.DEFAULT_AUTOCITI_QUEUE_HASH_MAP
        self._AutoCITIQueueHashMap = autoCITIQueueHashMap
            
        autoCITIJobTracker = self._ConfigManager.GetSettingValue('RedisHashMaps','AutoCITIJobTrackerHashMap')
        if not autoCITIJobTracker:
            autoCITIJobTracker = AutoCITIQueue.DEFAULT_AUTOCITI_JOB_TRACKER_HASH_MAP 
            self._AutoCITIJobTracker = autoCITIJobTracker
        
        priorityLevels = self._ConfigManager.GetSettingValue('QueueInfo','PriorityLevels')
        if not priorityLevels:
            priorityLevels = AutoCITIQueue.DEFAULT_PRIORITY_LEVELS
        self._PriorityLevels = priorityLevels.split(AutoCITIQueue.DEFAULT_PRIORITY_SEPERATOR)
        
        queueNameTemplate = self._ConfigManager.GetSettingValue('QueueInfo','QueueNameTemplate')
        if not queueNameTemplate:
            queueNameTemplate = AutoCITIQueue.DEFAULT_QUEUE_NAME_TEMPLATE
        self._QueueNameTemplate = queueNameTemplate
    
    def _AppSetup(self):
        configPath = resource_filename('AutoCITIQueue', 'Conf/AutoCITIQueue.conf')
        
        cfgMgr = ConfigManager.getInstance()
        cfgMgr.LoadConfigFile(configPath)
        
        userConfigPath = cfgMgr.GetSettingValue('General','UserConfigFilePath')
        
        if os.path.exists(userConfigPath):
            userConfigPath = AutoCITIQueue.DEFAULT_USER_CONF_PATH
        
        userConfigPath = GetAbsolutePath(userConfigPath)
        userConfigFolder = os.path.dirname(userConfigPath)
        if not os.path.exists(userConfigFolder):
            os.makedirs(userConfigFolder)
        
        cfgMgr.LoadConfigFile(userConfigPath)
        
        logConfigFilePath = resource_filename('AutoCITIQueue','Conf/AutoCITIQueue_LogConfig.conf')        
                    
        workingFolderPath = cfgMgr.GetSettingValue('General','WorkingFolderPath')
        if not os.path.exists(workingFolderPath):
            workingFolderPath = os.getcwd()
        workingFolderPath = os.path.expandvars(workingFolderPath)
        
        logsFolderName = cfgMgr.GetSettingValue('Logging','LogsFolderName')
        if not logsFolderName:
            logsFolderName = 'logs'
        logsFolderPath = os.path.join(workingFolderPath,logsFolderName)
        if not os.path.exists(logsFolderPath):
            os.makedirs(logsFolderPath)
        logFileName = cfgMgr.GetSettingValue('Logging','LogFileName')
        if not logFileName:
            logFileName = __name__
        logFilePath = os.path.join(logsFolderPath,logFileName)
        logging.config.fileConfig(logConfigFilePath, {"logFilePath":("%r" % logFilePath)}, False)
    
    def _ConnectRedis(self):
        redisHost = self._ConfigManager.GetSettingValue('RedisServer','Host')
        redisPort = self._ConfigManager.GetSettingValue('RedisServer','Port')
        redisDB = self._ConfigManager.GetSettingValue('RedisServer','DB')
        redisPassword = self._ConfigManager.GetSettingValue('RedisServer','Password')
        self._rdb = redis.Redis(redisHost,redisPort,redisDB,redisPassword)
        try:
            info = self._rdb.info()
            print self._rdb
            print info
            self._Logger.info("Connected Queue Connection Info %s" %str(info))
            self._connected = True
        except Exception as e:
            return False
        return True
    
    def _LoadQueues(self):
        if self._connected:
            queues = self._rdb.hgetall(self._AutoCITIQueueHashMap)
            for entity,queuename in queues.iteritems():
                self._AutoCITIQueues[entity] =queuename  
        else:
            raise Exception('Not Connected to redis server')
                
    def RegisterQueue(self,entity,queue):
        if self._connected:
            self._AutoCITIQueues[entity] = queue
            self._Logger.info('Entity %s with queue %s added to Hash Map %s' %(entity,queue,self._AutoCITIQueueHashMap))
            result = self._rdb.hset(self._AutoCITIQueueHashMap, entity, queue)
        else:
            raise Exception("Not Connected to redis server")
        return result
    
    def DeleteQueue(self,entity):
        if self._connected:
            if self._AutoCITIQueues.get(entity):
                result = self._rdb.hdel(self._AutoCITIQueueHashMap,entity)
                queue = self._AutoCITIQueues.pop(entity)
                self._Logger.info('Queue %s has been removed' %queue)
            else:
                raise Exception('Entity %s has no queue registered' %entity)
        else:
            raise Exception('Not Connected to redis server')
        return result
    
    def RemoveTestTracks(self,entity):
        if self._connected:
            queue = self._AutoCITIQueues(entity)
            if queue:
                for priority in self._PriorityLevels:
                    queueName = self._QueueNameTemplate%(queue,priority)
                    self._rdb.delete(queueName)
                    self._Logger.info('Removed all test tracks from queue %s' %queueName)
            else:
                raise Exception('Entity %s has no queue registered' %entity)
        else:
            raise Exception('Not Connected to redis server')
            
    
    
    def EnqueueTestTrack(self,testtrack,entity,priority):
        if self._connected:
            if self._AutoCITIQueues.get(entity):
                if priority and self._PriorityLevels.count(priority) >0 :
                    queueName = self._QueueNameTemplate%(self._AutoCITIQueues[entity],priority)
                    urn = uuid.uuid4().urn
                    task = Task(data=testtrack,urn=urn)
                    data = task.__dict__
                    self._rdb.lpush(queueName,json.dumps(data))
                    self._rdb.hset(self._AutoCITIJobTracker, urn, json.dumps({'status':'Queued','data':None}))
                    return urn
                else:
                    raise Exception('Priority %s not defined' %priority)
            else:
                raise Exception('Entity %s has no registered queue' %entity)  
        else:
            raise Exception('Not Connected to redis server')
        
    def DequeueTestTrack(self,entity,priority = None):
        if self._connected:
            if self._AutoCITIQueues.get(entity):
                queue = self._AutoCITIQueues[entity]
                if priority:
                    if self._PriorityLevels.count(priority) >0:
                        queuename = self._QueueNameTemplate%(queue,priority)
                        if self._rdb.llen(queuename) == 0:
                            return None
                        data = self._rdb.rpop(queuename)
                    else:
                        raise Exception('Priority %s not defined' %priority)
                else:
                    for priority in self._PriorityLevels:
                        queuename = self._QueueNameTemplate%(queue,priority)
                        if self._rdb.llen(queuename) == 0:
                            return None
                        else:
                            data = self._rdb.rpop(queuename)
                            break
                task = Task()
                task.__dict__ = json.loads(data)
                self._rdb.hset(self._AutoCITIJobTracker, task.urn, json.dumps({'status':'Executing','data':None}))
                return task
            else:
                raise Exception('Entity %s has no registered queue' %entity)
        else:
            raise Exception('Not connected to redis server')
    
    def send(self,task,result):
        if self._connected:
            if task.urn in self._rdb.hkeys(self._AutoCITIJobTracker):
                self._rdb.hset(self._AutoCITIJobTracker,task.urn, json.dumps({'status':'Completed','data':result}))
            else:
                raise Exception('Task not present')
        else:
            raise Exception('Not connected to redis server')
                
    def CheckStatus(self,urn):
        if self._connected:
            response = self._rdb.hget(self._AutoCITIJobTracker,urn)
            if response:
                responseJson = json.loads(response)
                if responseJson['status'] == 'Completed':
                    self._rdb.hdel(self._AutoCITIJobTracker,urn)
                return responseJson
            else:
                raise Exception('Task with urn %s not present' %urn)
        else:
            raise Exception('Not connected to redis server')
    
    def ClearPendingTasks(self):
        if self._connected:
            self._rdb.delete(self._AutoCITIJobTracker)
        else:
            raise Exception('Not connected to redis server')
        
    def EnqueuedTracks(self,entity):
        if self._connected:
            queue = self._AutoCITIQueues.get(entity)
            if queue:
                response = dict()
                for priority in self._PriorityLevels:
                    queueName = self._QueueNameTemplate%(queue,priority)
                    response[priority] = self._rdb.llen(queueName)                
            else:
                raise Exception('Entity %s has no registered queue' %entity)
        else:
            raise Exception('Not connected to redis server')
if __name__=="__main__":
    autocitiQueue = AutoCITIQueue()
    autocitiQueue.RegisterQueue('plang','plangQueue')
#     autocitiQueue.EnqueueTestTrack({'a':'3214','b':'ergeg'}, 'plang', 5)
    print autocitiQueue.DeleteQueue('plang')
    
        
        
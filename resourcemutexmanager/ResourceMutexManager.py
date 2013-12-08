'''
Created on 2/12/2013

@author: henry
'''
import redis
import threading
import time
import logging

RESOURCE_LOCK_TIMEOUT = 120 #seconds


class ResourceUnavailableError(Exception):
    pass

class ResourceMutexManager(object):
    
    def __init__(self, host='localhost', port=6379, db=0):
        self._host = host,
        self._port = port
        self._db = db
        self._redisClient = redis.StrictRedis(host=self._host, port=self._port, db=self._db)    
        self._resources = []
        self._thread = None
        self._alive = False
            
        self.log = logging.getLogger('ResourceMutexManager')
        self.log.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.log.addHandler(ch)
        
    @property
    def resources(self):
        return self._resources
        
    def waitFor(self, resources, blocking=True, retryInterval=30):
        """
        @param resources: a string representation of the resource to lock
        @param blocking: block until the resource is free
        @param retryInterval: int seconds between retry attempts
        @return: True if the lock has been acquired, False otherwise
        """
        self.log.debug("Waiting for %s retry interval: %d seconds" % (resources, retryInterval))
        resources = [resources] if isinstance(resources, str) else resources
        while self._resources != resources:
            for resource in resources:
                if self._redisClient.setnx(resource, RESOURCE_LOCK_TIMEOUT) == 1:
                    self.log.info("Acquired %s" % resource)
                    self._resources.append(resource)
                else:
                    self.log.debug("Failed to acquire %s" % resource)
            if not blocking and self._resources != resources:
                raise ResourceUnavailableError("Unable to acquire %s" % resource)
            elif self._resources != resources:
                self.releaseResources()
                self.log.debug("Retrying in %.02f seconds" % float(retryInterval))
                time.sleep(retryInterval)
        
        return len(resources) == len(self._resources)
    
    def releaseResources(self):
        """
        @return: True if all of the currently held resources were released
        """
        self.log.debug("Releasing locks on %s" % str(self._resources))
        numResourcesToRelease = len(self._resources)
        resourcesReleased = self._redisClient.delete(self._resources)
        if resourcesReleased == len(self._resources):
            self.log.debug("Released locks on %s" % str(self._resources))
        else:
            self.log.warning("Only released locks on %d out of %d resources" % (resourcesReleased, len(self._resources)))
        self._resources = []
        return resourcesReleased == numResourcesToRelease
    
    def _updateExpiryThread(self):
        pass
    
    def startUpdateExpiryThread(self):
        if self._thread is None or not self._thread.isAlive():
            self._alive = True
            self.log.debug("Starting update expiry thread")
            self._thread = threading.Thread(target=self._updateExpiryThread,
                                            name="%s expiry update thread" % str(self._resources))
            self._thread.start()
        else:
            self.log.debug("Update expiry thread is already running")
            
    def stopUpdateExpiryThread(self):
        startTime = time.time()
        self._alive = False
        self.log.info("Waiting for update expiry thread to stop")
        self._thread.join(2 * RESOURCE_LOCK_TIMEOUT)
        joinTime = int(time.time() - startTime)
        if self._thread.isAlive():
            self.log.warning("Update thread failed to join after %d seconds" % joinTime)
        else:
            self.log.debug("Update thread joined after %d seconds" % joinTime)
        
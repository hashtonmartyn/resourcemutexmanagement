'''
Created on 2/12/2013

:author: henry
'''
import redis
import threading
import time
import logging
import copy
from contextlib import contextmanager

RESOURCE_LOCK_TIMEOUT = 120     # seconds
EXPIRY_UPDATE_INTERVAL = 20     # seconds


class ResourceUnavailableError(Exception):
    pass

class ResourceMutexManager(object):
    
    def __init__(self, value="ResourceInUse", host='localhost', port=6379, db=0):
        """
        :param value: value to set the resource value to ie "descriptionOfThingUsingTheseResources"
        :param host: the address that the redis server is being hosted
        :param port: the port of the redis server
        :param db: the redis db
        """
        self._redisClient = redis.StrictRedis(host=host, port=port, db=db) 
        self._value = value   
        self._resources = []
        self._thread = None
        self._alive = False
        self._log = logging.getLogger(self.__class__.__name__)
        
    @property
    def resources(self):
        return copy.copy(self._resources)
        
    def waitFor(self, resources, blocking=True, retryInterval=30):
        """
        :param resources: a string representation of the resource to lock
        :param blocking: block until the resource is free
        :param retryInterval: int seconds between retry attempts
        :return: True if the lock has been acquired, False otherwise
        """
        self._log.debug("Waiting for %s retry interval: %d seconds" % (resources, retryInterval))
        resources = [resources] if isinstance(resources, str) else resources
        while self._resources != resources:
            for resource in resources:
                if self._redisClient.setnx(resource, self._value) == 1:
                    self._log.info("Acquired %s" % resource)
                    self._resources.append(resource)
                else:
                    self._log.debug("Failed to acquire %s" % resource)
            if not blocking and self._resources != resources:
                raise ResourceUnavailableError("Unable to acquire %s" % resource)
            elif self._resources != resources:
                if len(self._resources) > 0:
                    self.releaseResources()
                self._log.debug("Retrying in %.02f seconds" % float(retryInterval))
                time.sleep(retryInterval)
        
        return len(resources) == len(self._resources)
    
    @contextmanager
    def lock(self, resources):
        """
        :param resources: the resources to lock
        """
        self.waitFor(resources)
        self.startUpdateExpiryThread()
        yield
        self.stopUpdateExpiryThread()
        self.releaseResources()
    
    def releaseResources(self):
        """
        :return: True if all of the currently held resources were released
        """
        self._log.debug("Releasing locks on %s" % str(self._resources))
        numResourcesToRelease = len(self._resources)
        resourcesReleased = self._redisClient.delete(*self._resources)
        if resourcesReleased == len(self._resources):
            self._log.debug("Released locks on %s" % str(self._resources))
        else:
            self._log.warning("Only released locks on %d out of %d resources" % (resourcesReleased, len(self._resources)))
        self._resources = []
        return resourcesReleased == numResourcesToRelease
    
    def _updateExpiryThread(self):
        """
        Don't call this directly
        """
        while self._alive:
            for resource in self._resources:
                if self._redisClient.expire(resource, RESOURCE_LOCK_TIMEOUT) == 0:
                    self._log.warning("Lock for %s has expired, attempting to re-acquire it")
                    if self._redisClient.setnx(resource, self._value) == 1:
                        self._log.debug("Successfully re-acquired %s" % resource)
                    else:
                        self._log.warning("Failed to re-acquire %s" % resource)
                else:
                    self._log.debug("Updated expiry for %s" % resource)
            time.sleep(EXPIRY_UPDATE_INTERVAL)
        self._log.debug("Update expiry thread terminated")
    
    def startUpdateExpiryThread(self):
        """
        Starts a thread to update the expiry of each resource's lock
        :precondition: all resources are acquired before calling this
        """
        if self._thread is None or not self._thread.isAlive():
            self._alive = True
            self._log.debug("Starting update expiry thread")
            self._thread = threading.Thread(target=self._updateExpiryThread,
                                            name="%s expiry update thread" % str(self._resources))
            self._thread.start()
        else:
            self._log.debug("Update expiry thread is already running")
            
    def stopUpdateExpiryThread(self):
        """
        Stops the thread which updates each resource's lock expiry. You should probably call
        releaseResources after this. Or not, your call since the resource lock should 
        expire at some point anyway.
        """
        startTime = time.time()
        self._alive = False
        self._log.info("Waiting for update expiry thread to stop")
        self._thread.join(2 * RESOURCE_LOCK_TIMEOUT)
        joinTime = time.time() - startTime
        if self._thread.isAlive():
            self._log.warning("Update expiry thread failed to join after %.02f seconds" % joinTime)
        else:
            self._log.debug("Update expiry thread joined after %.02f seconds" % joinTime)
            
if __name__ == "__main__":
    res = str(time.time())
    rmm = ResourceMutexManager()
    with rmm.lock(res) as f:
        print "locked"
    print "not locked"
    
    rmm.waitFor(res)
    rmm.startUpdateExpiryThread()
    time.sleep(10)
    rmm.stopUpdateExpiryThread()
    rmm.releaseResources()
    
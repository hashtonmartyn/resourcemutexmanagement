'''
Created on 2/12/2013

@author: henry
'''
import mock
from mock import patch
from nose import with_setup
from nose.tools import raises
import unittest
import threading
from ResourceMutexManager import ResourceMutexManager, ResourceUnavailableError

RESOURCE_ONE = "Resource1"
RESOURCE_TWO = "Resource2"


class Test_ResourceLock(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        super(Test_ResourceLock, self).__init__(*args, **kwargs)
        self.lockManager = None

    @patch("redis.StrictRedis")
    def setUp(self, patch):
        self.lockManager = ResourceMutexManager()
        self.lockManager._redisClient.setnx.return_value = 1
    
    def tearDown(self):
        pass
    
    @with_setup(setUp, tearDown)
    def test_waitForLock_defaultArgs_acquiresLockImmediately(self):
        self.assertTrue(self.lockManager.waitFor(RESOURCE_ONE),
                        "waitForLock should return True once the lock is acquired")
        self.assertTrue(RESOURCE_ONE in self.lockManager.resources)
        self.lockManager._redisClient.setnx.assert_called_once()
        self.assertEqual(2, len(self.lockManager._redisClient.setnx.call_args))

        self.assertTrue(RESOURCE_ONE in str(self.lockManager._redisClient.setnx.call_args),
                        self.lockManager._redisClient.setnx.call_args)
        
    @with_setup(setUp, tearDown)
    def test_waitForLock_multipleResources_allResourcesAcquired(self):
        resources = [RESOURCE_ONE, RESOURCE_TWO]
        self.assertTrue(self.lockManager.waitFor(resources),
                        "waitForLock should return True when all resource locks are acquired")
        self.assertTrue(self.lockManager.resources == resources)
        for resource in resources:
            self.assertTrue(self.lockManager._redisClient.setnx.called_once_with(resource))
            
    @with_setup(setUp, tearDown)
    @raises(ResourceUnavailableError)
    def test_waitForLock_resourceUnavailableNotBlocking_raisesException(self):
        self.lockManager._redisClient.setnx.return_value = 0
        self.lockManager.waitFor(RESOURCE_ONE, blocking=False)
        
    @with_setup(setUp, tearDown)
    def test_releaseLock_oneLockAcquired_lockReleased(self):
        self.lockManager._redisClient.delete.return_value = 1
        self.assertTrue(self.lockManager.waitFor(RESOURCE_ONE))
        self.assertTrue(RESOURCE_ONE in self.lockManager.resources)
        self.assertTrue(self.lockManager.releaseResources())
        self.assertTrue(self.lockManager._redisClient.delete.called_with([RESOURCE_ONE]))
        self.assertTrue(self.lockManager.resources == [])
        
    @with_setup(setUp, tearDown)
    def test_releaseLock_twoLocksAcquired_twoLocksReleased(self):
        self.lockManager._redisClient.delete.return_value = 2
        resources = [RESOURCE_ONE, RESOURCE_TWO]
        self.assertTrue(self.lockManager.waitFor(resources))
        self.assertTrue(resources == self.lockManager.resources)
        self.assertTrue(self.lockManager.releaseResources())
        self.assertTrue(self.lockManager._redisClient.delete.called_with(resources))
        self.assertTrue(self.lockManager.resources == [])
        
    @with_setup(setUp, tearDown)
    def test_releaseLock_noLockAcquired_returnsFalse(self):
        self.assertFalse(self.lockManager.releaseResources(),
                         "No resource locks acquired so none should have been deleted")
        self.lockManager._redisClient.delete.assert_called_once_with([])
        
    @with_setup(setUp, tearDown)
    @patch("ResourceMutexManager.ResourceMutexManager._updateExpiryThread")
    def test_startUpdateExpiryThread_startsThreadWhenThreadIsNone(self, patch):
        self.lockManager.startUpdateExpiryThread()
        self.assertTrue(self.lockManager._updateExpiryThread.called_once())
        
    @with_setup(setUp, tearDown)
    @patch("ResourceMutexManager.ResourceMutexManager._updateExpiryThread")
    @patch("threading.Thread")
    def test_startUpdateExpiryThread_startThreadWhenThreadIsntAlive(self, resourceLockManagerPatch, threadingPatch):
        self.lockManager._thread = threading.Thread()
        self.lockManager._thread.isAlive.returns(False)
        self.lockManager.startUpdateExpiryThread()
        self.assertTrue(self.lockManager._updateExpiryThread.call_once())
        
    @with_setup(setUp, tearDown)
    @patch("ResourceMutexManager.ResourceMutexManager._updateExpiryThread")
    @patch("threading.Thread")
    def test_startUpdateExpiryThread_doesntStartThreadWhenThreadIsAlive(self, resourceLockManagerPatch, threadingPatch):
        self.lockManager._thread = threading.Thread()
        self.lockManager._thread.isAlive.returns(True)
        self.lockManager.startUpdateExpiryThread()
        self.assertTrue(self.lockManager._updateExpiryThread.call_count == 0)
        
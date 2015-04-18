ResourceMutexManagement, RMM, lets you control access to resources that can't be used concurrently. It's written in Python and depends on redis-py https://pypi.python.org/pypi/redis/ and a redis server being run.

If you have n tests that can/do run concurrently but all require access to a resource that can't be used by more than one test at a time, this could be the package for you.

https://pypi.python.org/pypi/ResourceMutexManagement

Install with: pip install ResourceMutexManagement

Code example:
```
from ResourceMutexManager import ResourceMutexManager
someResource = "Resource1"
rmm = ResourceMutexManager()
rmm.waitFor(someResource)
rmm.startUpdateExpiryThread()
rmm.stopUpdateExpiryThread()
rmm.releaseResources()
```
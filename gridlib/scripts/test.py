from joblib.hpc import versioned, LocalSpawnExecutor
from joblib.hpc.clusters.titan_oslo import TitanOsloExecutor
from joblib.hpc.test.funcs import func1, funcex, download_url
import logging
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#e = TitanOsloExecutor(account='quiet', logger=logger)
e = LocalSpawnExecutor(logger=logger, poll_interval=.1)

url_future = e.submit(download_url, 'http://docs.python.org/library/urllib.html', 'urllib.html')
print url_future.result().abspath()


fut1 = e.submit(func1, 2, 3)
fut2 = e.submit(funcex, 2, 3)
print 'trying to get result'
print fut1.result()
print type(fut2.exception(100))
print fut2.result(100)



# TODO check function hash

# TODO detach daemon in process spawner:
# http://stackoverflow.com/questions/972362/spawning-process-from-python

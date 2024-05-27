import os
import time
import datetime as dt
import pickle
import logging

logger = logging.getLogger(__name__)


def need_to_download(fn, expire_time_secs, force_reload = False, only_cached = False):
        file_exists = os.path.exists(fn)

        if file_exists:
            last_fetch = dt.datetime.fromtimestamp(os.path.getmtime(fn))
        else:
            last_fetch = dt.datetime.now() - dt.timedelta(seconds = expire_time_secs + 1)

        now = dt.datetime.now()
        file_expired = now - last_fetch > dt.timedelta(seconds = expire_time_secs)
        return (file_expired and not only_cached) or not file_exists or force_reload

class Cachy:
    def __init__(self, expire_time_in_secs, path=".", only_cached = False, force_reload = False, cache_failure = False):
        self.expire_time_in_secs = expire_time_in_secs
        self.path = path
        self.only_cached = only_cached
        self.force_reload = force_reload
        self.cache_failure = cache_failure

    def build(download_f, save_f, read_f):
        return Cachedf(download_f, save_f, read_f, self.expire_time_secs, self.path, self.only_cached, self.force_reload, self.cache_failure)

    def pickle_build(self, f):
        return pickle_cached(f, self.expire_time_in_secs, self.path)
    


"""
    Returns callable objects
"""
class Cachedf(object):
    """
        * only_cached -- do not call `download_f` even if it is expired. Downloads only if there's no file
        * force_reload -- call `download_f` even if it has not expired yet. Overrides `only_cached`
        * cache_failure -- don't try to reload if failure is recent
        * use_stale_on_failure -- in case download fails, read old file
    """
    def __init__(self, download_f, save_f, read_f,
            expire_time_secs,
            path = ".", 
            only_cached = False, 
            force_reload = False, 
            cache_failure = True,
            use_stale_on_failure = False
            ):
        self.download_f = download_f
        self.save_f = save_f
        self.read_f = read_f
        self.expire_time_secs = expire_time_secs
        self.only_cached = only_cached
        self.force_reload = force_reload
        self.path = path
        self.cache_failure = cache_failure
        self.use_stale_on_failure = use_stale_on_failure
    

    def __call__(self, *args, **kwargs):
        fn = args[0]
        fn = os.path.join(self.path, fn)
        fail_file = "%s.fail" % (fn,)

        os.makedirs(self.path, exist_ok=True)

        file_exists = os.path.exists(fn)
        fail_file_exists = os.path.exists(fail_file)

        if file_exists:
            last_fetch = dt.datetime.fromtimestamp(os.path.getmtime(fn))
        else:
            last_fetch = dt.datetime.now() - dt.timedelta(seconds = self.expire_time_secs + 1)

        """
            Even if file exists, we might've failed on last
            fetch attempt, thus we need to get latest date of
            two
        """
        if fail_file_exists:
            ff_last_fetch = dt.datetime.fromtimestamp(os.path.getmtime(fail_file))
            last_fetch = max(ff_last_fetch, last_fetch)

        now = dt.datetime.now()
        file_expired = now - last_fetch > dt.timedelta(seconds = self.expire_time_secs) 

        no_file = not file_exists
        no_fail_file = not fail_file_exists
        expired_file = (file_exists or fail_file_exists) and file_expired and not self.only_cached

        if expired_file:
            logger.debug("File expired. Will download")
        if no_file and no_fail_file:
            logger.debug("No neither file nor fail file. Will download")
        if self.force_reload:
            logger.debug("Forcing reload. Will download")

        if expired_file or no_file or self.force_reload:
            fargs = args[1:]
            try:
                r = self.download_f(*fargs, **kwargs)
                self.save_f(fn, r)
            except Exception as e:
                if self.cache_failure:
                    with open(fail_file, "w") as f:
                        f.write(now.isoformat())
                
                if not self.use_stale_on_failure:
                    raise e

                if no_file:
                    logger.debug("There's no file present and download failed, nothing to read data from at all")
                    # can't do anything here
                    raise e

                logger.debug("Using old data")
                r = self.read_f(fn)
        else:
            r = self.read_f(fn)
        return r

class Throttledf(object):
    def __init__(self, f, calls, wait_secs, interval_secs):
        """
            You should set allowed count of `calls` per `interval_secs`.
            This means each `calls`+1 call will be delayed by `wait_secs`
        """
        self.cc = 0
        self.wait_secs = wait_secs
        self.calls = calls
        self.interval_secs = interval_secs
        self.f = f
        self.on_throttle = None

    def __call__(self, *args, **kwargs):
        if (self.cc + 1) % (self.calls+1) == 0:
            if self.on_throttle is not None:
                self.on_throttle(self)
            time.sleep(self.wait_secs)

        re = None
        try:
            r = self.f(*args, **kwargs)
        except Exception as e:
            re = e
        self.cc += 1
        if re is not None:
            raise re
        else:
            return r

def throttle(f, calls, interval_secs, wait_secs):
    t = Throttledf(f, calls, wait_secs, interval_secs)
    return t


def pickle_save(fn, data):
    with open(fn, "wb") as f:
        pickle.dump(data, f)

def pickle_read(fn):
    with open(fn, "rb") as f:
        return pickle.load(f)

def pickle_cached(f, expire_time_in_secs, cached_dir = None):
    if not cached_dir:
        return Cachedf(f, pickle_save, pickle_read, expire_time_in_secs)    
    return Cachedf(f, pickle_save, pickle_read, expire_time_in_secs, cached_dir)

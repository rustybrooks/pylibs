import datetime
import decimal
import functools
import hashlib
import json
import logging
import os

import dateutil.parser

cache_sentinel = object()
logger = logging.getLogger(__name__)

CACHE = 1
NOCACHE = 2
PRECACHE = 3
RECACHE = 4


def ourbytes(x):
    return x.encode(encoding="utf-8")


def arg_hash_gen(skip=None):
    skip = skip or []

    def fn(*args, **kwargs):
        h = hashlib.md5()
        list(map(lambda x: h.update(ourbytes(str(x))), args))
        list(
            map(
                lambda x: h.update(ourbytes(str(x) + str(kwargs[x]))), sorted(kwargs.keys())
            )
        )
        return h.hexdigest()

    return fn


arg_hash = arg_hash_gen([])


class OurJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "to_json"):
            return obj.to_json()
        elif isinstance(
            obj, (datetime.datetime.__class__, datetime.date)
        ):  # this __class__ nonsense is due to testing and Mocks
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return json.JSONEncoder.default(self, obj)


def default_cachefn(*args, **kwargs):
    if kwargs.get("_precache", False):
        return PRECACHE

    if kwargs.get("_nocache", False):
        return NOCACHE

    if kwargs.get("_recache", False):
        return RECACHE

    return CACHE


# a canonical cache object is a dict or dict-like object with the keys of
# key, created, value, args, kwargs
class CacheBase(object):
    def __init__(
        self,
        prefix,
        timeout,
        grace=None,
        keyfn=None,
        cachefn=None,
        binary=False,
        debug=False,
    ):
        self.prefix = prefix
        self.timeout = timeout
        self.grace = grace
        self.keyfn = keyfn or arg_hash_gen()
        self.cachefn = cachefn or default_cachefn
        self.binary = binary
        self.debug = debug

    #############################################################################
    # functions subclasses must implement

    # needs to save a canonical cache object
    def update_cache(self, key, cache):
        raise Exception("Not implemented")

    # needs to load and return a canonical cache object
    def load_cache(self, key):
        raise Exception("Not implemented")

    # needs to check for the existence of the cached object
    def exists_cache(self, key):
        raise Exception("Not implemented")

    # needs to remove item from cache
    def delete_cache(self, key):
        raise Exception("Not implemented")

    # needs to return list of items
    def keys(self):
        raise Exception("Not implemented")

    def key_from_args(self, *args, **kwargs):
        stripped_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ["_precache", "_nocache", "_recache"]
        }
        return self.keyfn(*args, **stripped_kwargs), stripped_kwargs

    #############################################################################
    # global cache functions

    # def exists(self, *args, **kwargs):
    #     key, stripped_kwargs = self.key_from_args(*args, **kwargs)
    #     return self.exists_cache(key=key)

    def expired_items(self):
        for key in self.keys():
            cache = self.load_cache(key)
            diff = (datetime.datetime.utcnow() - cache["created"]).total_seconds()
            if diff > self.timeout:
                yield cache

    def need_refresh_items(self):
        if not self.grace:
            return

        for key in self.keys():
            cache = self.load_cache(key)
            diff = (datetime.datetime.utcnow() - cache["created"]).total_seconds()
            if self.timeout - self.grace < diff:
                yield cache

    def delete_all(self):
        for key in list(self.keys()):
            self.delete_cache(key)

    def delete_expired(self):
        for cache in self.expired_items():
            if self.debug:
                logger.warning(
                    "Deleting fn=%r, key=%r, args=%r, kwargs=%r",
                    cache["key"],
                    cache.get("args"),
                    cache.get("kwargs"),
                )
            self.delete_cache(key=cache["key"])

    def refresh_cache(self, fn):
        for cache in self.need_refresh_items():
            if self.debug:
                logger.warning(
                    "Refreshing fn=%r, key=%r, args=%r, kwargs=%r",
                    fn.__name__,
                    cache["key"],
                    cache.get("args"),
                    cache.get("kwargs"),
                )
            fn(*cache["args"], **cache["kwargs"])

    def __call__(self, fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            key, stripped_kwargs = self.key_from_args(*args, **kwargs)
            cachefnval = self.cachefn(*args, **kwargs) if self.cachefn else CACHE

            if self.debug:
                logger.warning(
                    "[cache - %r] CACHE fn=%r key=%r args=%r kwargs=%r stripped=%r",
                    self.prefix,
                    fn.__name__,
                    key,
                    args,
                    kwargs,
                    stripped_kwargs,
                )

            if cachefnval == NOCACHE:
                if self.debug:
                    logger.warning(
                        "[cache - %r] NOCACHE fn=%r key=%r",
                        self.prefix,
                        fn.__name__,
                        key,
                    )
                return fn(*args, **stripped_kwargs)

            if cachefnval == PRECACHE:
                if self.debug:
                    logger.warning(
                        "[cache - %r] PRECACHE fn=%r key=%r",
                        self.prefix,
                        fn.__name__,
                        key,
                    )
                cached = None
            else:
                cached = self.load_cache(key)

            diff = (
                (datetime.datetime.utcnow() - cached["created"]).total_seconds()
                if cached
                else 1e9
            )

            do_cache = False
            if not cached:
                do_cache = True
                if self.debug:
                    logger.warning(
                        "[cache - %r] No cached value, creating cache fn=%r, key=%r",
                        self.prefix,
                        fn.__name__,
                        key,
                    )
            elif diff > self.timeout:
                do_cache = True
                if self.debug:
                    logger.warning(
                        "[cache - %r] Timed out, creating cache fn=%r, key=%r, diff=%r",
                        self.prefix,
                        fn.__name__,
                        key,
                        diff,
                    )
            elif (
                cachefnval == RECACHE
                and self.grace
                and self.timeout - self.grace < diff
            ):
                do_cache = True
                if self.debug:
                    logger.warning(
                        "[cache - %r] RECACHE, re-creating cache fn=%r, key=%r diff=%r",
                        self.prefix,
                        fn.__name__,
                        key,
                        diff,
                    )

            if do_cache:
                val = fn(*args, **stripped_kwargs)
                cached = {
                    "key": key,
                    "created": datetime.datetime.utcnow(),
                    "value": bytes(val) if self.binary else val,
                    "args": args,
                    "kwargs": stripped_kwargs,
                }
                self.update_cache(key, cached)

            return cached["value"]

        return wrapper


class MemoryCache(CacheBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = {}

    def update_cache(self, key, cache):
        self.data[key] = cache

    def load_cache(self, key):
        return self.data.get(key)

    def exists_cache(self, key):
        return key in self.data

    def delete_cache(self, key):
        try:
            del self.data[key]
        except KeyError:
            pass

    def keys(self):
        return self.data.keys()


class FileCache(CacheBase):
    def __init__(self, basedir, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.basedir = basedir

    def cache_file(self, key, makedir=False):
        root = os.path.join(self.basedir, self.prefix)
        if makedir and not os.path.exists(root):
            os.makedirs(root)
        file_name = os.path.join(root, key)
        return file_name

    def update_cache(self, key, cache):
        with open(self.cache_file(key, makedir=True), "w+") as f:
            json.dump(cache, f, cls=OurJSONEncoder, indent=2)

    def load_cache(self, key):
        if not self.exists_cache(key):
            return None

        with open(self.cache_file(key), "r+") as f:
            val = json.load(f)

        if val:
            val["created"] = dateutil.parser.parse(val["created"])

        return val

    def exists_cache(self, key):
        cache_file = self.cache_file(key)
        return os.path.exists(cache_file)

    def delete_cache(self, key):
        try:
            os.unlink(self.cache_file(key))
        except KeyError:
            pass

    def keys(self):
        return os.listdir(os.path.join(self.basedir, self.prefix))


class MongoCache(CacheBase):
    def __init__(self, mongo, prefix, *args, **kwargs):
        super().__init__(prefix, *args, **kwargs)
        self.mongo_table = "cache_" + prefix
        self.mongo = mongo

    def update_cache(self, key, cache):
        self.mongo.db[self.mongo_table].update({"key": key}, cache, upsert=True)

    def load_cache(self, key):
        return self.mongo.safedb[self.mongo_table].find_one({"key": key})

    def exists_cache(self, key):
        val = self.mongo.safedb[self.mongo_table].find_one({"key": key}, {"_id": 1})
        return val is not None

    def delete_cache(self, key):
        self.mongo.safedb[self.mongo_table].delete_one({"key": key})

    def keys(self):
        return (
            x["key"]
            for x in self.mongo.safedb[self.mongo_table].find({}, {"_id": 0, "key": 1})
        )


class MysqlCache(CacheBase):
    def __init__(self, sql, prefix, *args, **kwargs):
        super().__init__(prefix, *args, **kwargs)
        self.sql = sql
        self.prefix = prefix
        self.prefix_key = hashlib.md5(bytes(prefix, encoding="ascii")).hexdigest()

    def _cache_key(self, key):
        return "{}:{}".format(self.prefix_key, key)

    def update_cache(self, key, cache):
        query = """
            insert into caches(cache_key, value, expiration) 
            values(%s, %s, %s) 
            on duplicate key update value=values(value), expiration=values(expiration)
        """
        expiration = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=self.timeout
        )
        bindvars = [
            self._cache_key(key),
            json.dumps(cache, cls=OurJSONEncoder),
            expiration,
        ]
        self.sql.execute(query, bindvars)

    def load_cache(self, key):
        val = self.sql.select_0or1(
            "select * from caches where cache_key=%s", [self._cache_key(key)]
        )
        if val:
            v = json.loads(val.value)
            v["created"] = dateutil.parser.parse(v["created"])
            return v

        return None

    def exists_cache(self, key):
        return self.load_cache(key) is not None

    def delete_cache(self, key):
        self.sql.delete("caches", "cache_key=%s", [self._cache_key(key)])

    def keys(self):
        return list(
            self.sql.select_column(
                "select cache_key from caches where cache_key like %s",
                [self.prefix_key + ":"],
            )
        )

    @classmethod
    def add_migration(cls, migration_obj):
        migration_obj.add_statement(
            """
            create table caches(
                cache_key char(65) not null primary key,
                value mediumtext,
                expiration datetime
            )
        """
        )

    @classmethod
    def migration_delete_tables(cls):
        return ["caches"]


class RedisCache(CacheBase):
    def __init__(self, connfn, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connfn = connfn

    def _conn(self):
        return self.connfn()

    def _key(self, keystr):
        return "cache:{}:{}".format(self.prefix, keystr)

    def update_cache(self, key, cache):
        key = self._key(key)
        value = json.dumps(cache, cls=OurJSONEncoder)
        if self.debug:
            logger.warning("redis update_cache key=%r, timeout=%r", key, self.timeout)
        with self._conn() as redis:
            redis.set(key, value, ex=self.timeout)

    def load_cache(self, key):
        with self._conn() as redis:
            val = redis.get(self._key(key))

        if val is not None:
            val = json.loads(val)
            val["created"] = dateutil.parser.parse(val["created"])

        if self.debug:
            logger.warning(
                "[cache - %r] redis load_cache key=%r, timeout=%r, exists=%r, created=%r",
                self.prefix,
                key,
                self.timeout,
                val is not None,
                val["created"] if val else None,
            )
        return val

    def exists_cache(self, key):
        with self._conn() as redis:
            return redis.exists(self._key(key))

    def delete_cache(self, key):
        with self._conn() as redis:
            redis.delete(self._key(key))

    def keys(self):
        with self._conn() as redis:
            for key in redis.scan_iter(self._key("*")):
                yield ":".join(key.decode("utf-8").split(":")[2:])


class S3Cache(CacheBase):
    def __init__(self, bucket_name, client, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.bucket_name = bucket_name
        self.client = client

    def _key(self, keystr):
        return "cache/{}/{}".format(self.prefix, keystr)

    def _upload(self, key, content=None, **kwargs):
        self.client.put_object(Body=content, Bucket=self.bucket_name, Key=key, **kwargs)
        return key

    def _download(self, key):
        try:
            return self.client.get_object(Bucket=self.bucket_name, Key=key)[
                "Body"
            ].read()
        except self.client.exceptions.NoSuchKey:
            pass
        return None

    def _list_keys(self, prefix=None):
        if prefix:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )
        else:
            response = self.client.list_objects_v2(Bucket=self.bucket_name)
        if "Contents" not in response:
            return
        return [item["Key"] for item in response["Contents"]]

    def _exists_key(self, key):
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def update_cache(self, key, cache):
        key = self._key(key)
        value = json.dumps(cache, cls=OurJSONEncoder)
        self._upload(key=key, content=value)

    def load_cache(self, key):
        key = self._key(key)
        bk = self._download(key)
        if bk is None:
            return None

        val = json.loads(bk)
        val["created"] = dateutil.parser.parse(val["created"])
        return val

    def exists_cache(self, key):
        key = self._key(key)
        return self._exists_key(key) is not None

    def delete_cache(self, key):
        key = self._key(key)
        self._delete_key(key)

    def keys(self):
        for key in self._list_keys():
            yield "/".join(key.split("/")[2:])


class PostgresCache(CacheBase):
    def __init__(self, sql, prefix, *args, **kwargs):
        super().__init__(prefix, *args, **kwargs)
        self.sql = sql
        self.prefix = prefix
        self.prefix_key = hashlib.md5(prefix).hexdigest()

    def _cache_key(self, key):
        return "{}:{}".format(self.prefix_key, key)

    def update_cache(self, key, cache):
        query = """
            insert into caches(cache_key, value, expiration) 
            values(%s, %s, %s) 
            on duplicate key update value=values(value), expiration=values(expiration)
        """
        expiration = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=self.timeout
        )
        bindvars = [
            self._cache_key(key),
            json.dumps(cache, cls=OurJSONEncoder),
            expiration,
        ]
        self.sql.execute(query, bindvars)

    def load_cache(self, key):
        val = self.sql.select_0or1(
            "select * from caches where cache_key=%s", [self._cache_key(key)]
        )
        if val:
            v = json.loads(val.value)
            v["created"] = dateutil.parser.parse(v["created"])
            return v

        return None

    def exists_cache(self, key):
        return self.load_cache(key) is not None

    def delete_cache(self, key):
        self.sql.delete("caches", "cache_key=%s", [self._cache_key(key)])

    def keys(self):
        return list(
            self.sql.select_column(
                "select cache_key from caches where cache_key like %s",
                [self.prefix_key + ":"],
            )
        )

    @classmethod
    def add_migration(cls, migration_obj):
        migration_obj.add_statement(
            """
            create table caches(
                cache_key char(65) not null primary key,
                value json,
                expiration timestamp
            )
        """
        )

    @classmethod
    def migration_delete_tables(cls):
        return ["caches"]

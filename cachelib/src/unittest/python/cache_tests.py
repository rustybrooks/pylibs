import datetime
from mock import Mock, patch
from unittest import TestCase
import boto3

import cachelib as cache


def slow_function(a1, a2, k1=1, k2=2):
    return [cache.datetime.datetime.utcnow(), a1, a2, k1, k2]


class CacheMockMemory(cache.MemoryCache):
    def __init__(self, *args, **kwargs):
        super().__init__(prefix="", *args, **kwargs)
        self.log = []

    def update_cache(self, key, cache):
        super().update_cache(key, cache)
        self.log.append(["update_cache", key, cache])

    def load_cache(self, key):
        self.log.append(["load_cache", key])
        return super().load_cache(key)

    def exists_cache(self, key):
        self.log.append(["exists_cache", key])
        return super().exists_cache(key)


class CacheMockFile(cache.FileCache):
    def __init__(self, *args, **kwargs):
        super().__init__(prefix="", basedir="/tmp/cache", debug=False, *args, **kwargs)
        self.log = []

    def update_cache(self, key, cache):
        super().update_cache(key, cache)
        self.log.append(["update_cache", key, cache])

    def load_cache(self, key):
        self.log.append(["load_cache", key])
        return super().load_cache(key)

    def exists_cache(self, key):
        self.log.append(["exists_cache", key])
        return super().exists_cache(key)


class TestCacheBase(TestCase):
    def test_cache_base(self):
        c = cache.CacheBase(prefix="foo", timeout=60)
        with self.assertRaises(Exception):
            c.update_cache()
        with self.assertRaises(Exception):
            c.load_cache()
        with self.assertRaises(Exception):
            c.exists_cache()
        with self.assertRaises(Exception):
            c.delete_cache()
        with self.assertRaises(Exception):
            c.keys()

    def test_memory(self, debug=True):
        d1 = datetime.datetime(2017, 1, 1)

        dtmock = Mock(wraps=datetime.datetime)

        cm = CacheMockMemory(timeout=60, grace=15, debug=debug)
        fn = cm(slow_function)

        # nothing in cache, should check cache and save value
        dtmock.utcnow = Mock(return_value=d1)
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo")
            self.assertEqual(val, [datetime.datetime(2017, 1, 1, 0, 0), 1, 2, "foo", 2])
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                    [
                        "update_cache",
                        "671a8e4f0f414e06565dff65ec7e3a8e",
                        {
                            "kwargs": {"k1": "foo"},
                            "args": (1, 2),
                            "value": val,
                            "key": "671a8e4f0f414e06565dff65ec7e3a8e",
                            "created": datetime.datetime(2017, 1, 1, 0, 0),
                        },
                    ],
                ],
            )
            self.assertEqual(sorted(cm.keys()), ["671a8e4f0f414e06565dff65ec7e3a8e"])

        # same function but different params, should also run function and store in cache
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=1))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="bar")
            self.assertEqual(
                val, [datetime.datetime(2017, 1, 1, 0, 0, 1), 1, 2, "bar", 2]
            )
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "3e474044f9e1663185a788015e91b325"],
                    [
                        "update_cache",
                        "3e474044f9e1663185a788015e91b325",
                        {
                            "kwargs": {"k1": "bar"},
                            "args": (1, 2),
                            "value": val,
                            "key": "3e474044f9e1663185a788015e91b325",
                            "created": datetime.datetime(2017, 1, 1, 0, 0, 1),
                        },
                    ],
                ],
            )

        # back to first function, within cache timeout, should just return cached value
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=2))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo")
            self.assertEqual(val, [datetime.datetime(2017, 1, 1, 0, 0), 1, 2, "foo", 2])
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                ],
            )

        # run with nocache, should skip caching stuff altogether, but not disrupt cache
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=3))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo", _nocache=True)
            self.assertEqual(
                val, [datetime.datetime(2017, 1, 1, 0, 0, 3), 1, 2, "foo", 2]
            )
            self.assertEqual(cm.log, [])
            self.assertEqual(
                sorted(cm.keys()),
                [
                    "3e474044f9e1663185a788015e91b325",
                    "671a8e4f0f414e06565dff65ec7e3a8e",
                ],
            )

            val = fn(1, 2, k1="foo")
            self.assertEqual(val, [datetime.datetime(2017, 1, 1, 0, 0), 1, 2, "foo", 2])
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                ],
            )

        # run with precache, which should update cache even though it's not expired
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=10))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo", _precache=True)
            self.assertEqual(
                val, [datetime.datetime(2017, 1, 1, 0, 0, 10), 1, 2, "foo", 2]
            )
            self.assertEqual(
                cm.log,
                [
                    [
                        "update_cache",
                        "671a8e4f0f414e06565dff65ec7e3a8e",
                        {
                            "kwargs": {"k1": "foo"},
                            "args": (1, 2),
                            "value": val,
                            "key": "671a8e4f0f414e06565dff65ec7e3a8e",
                            "created": datetime.datetime(2017, 1, 1, 0, 0, 10),
                        },
                    ]
                ],
            )

        # run right before cache should expire (59s after last precache)z
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=69))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo")
            self.assertEqual(
                val, [datetime.datetime(2017, 1, 1, 0, 0, 10), 1, 2, "foo", 2]
            )
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                ],
            )

        # run right after cache should expire (59s after last precache), should re-cache
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=71))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo")
            self.assertEqual(
                val, [datetime.datetime(2017, 1, 1, 0, 1, 11), 1, 2, "foo", 2]
            )
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                    [
                        "update_cache",
                        "671a8e4f0f414e06565dff65ec7e3a8e",
                        {
                            "kwargs": {"k1": "foo"},
                            "args": (1, 2),
                            "value": val,
                            "key": "671a8e4f0f414e06565dff65ec7e3a8e",
                            "created": datetime.datetime(2017, 1, 1, 0, 1, 11),
                        },
                    ],
                ],
            )

        # run with recache=True, but before recache period, should just return cached value
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=71 + 44))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo", _recache=True)
            self.assertEqual(
                val, [datetime.datetime(2017, 1, 1, 0, 1, 11), 1, 2, "foo", 2]
            )
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                ],
            )

        # run with recache=True, but after recache period, should regen cache
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=71 + 46))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo", _recache=True)
            self.assertEqual(
                val, [datetime.datetime(2017, 1, 1, 0, 1, 57), 1, 2, "foo", 2]
            )
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                    [
                        "update_cache",
                        "671a8e4f0f414e06565dff65ec7e3a8e",
                        {
                            "kwargs": {"k1": "foo"},
                            "args": (1, 2),
                            "value": val,
                            "key": "671a8e4f0f414e06565dff65ec7e3a8e",
                            "created": datetime.datetime(2017, 1, 1, 0, 1, 57),
                        },
                    ],
                ],
            )

        self.assertTrue(cm.exists_cache("3e474044f9e1663185a788015e91b325"))
        self.assertFalse(cm.exists_cache("00000000000000000000000000000000"))

        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=5))
        with patch("cachelib.datetime.datetime", dtmock):
            self.assertEqual([x["key"] for x in cm.expired_items()], [])
            self.assertEqual([x["key"] for x in cm.need_refresh_items()], [])

        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=500))
        with patch("cachelib.datetime.datetime", dtmock):
            self.assertEqual(
                [x["key"] for x in cm.expired_items()],
                [
                    "671a8e4f0f414e06565dff65ec7e3a8e",
                    "3e474044f9e1663185a788015e91b325",
                ],
            )
            self.assertEqual(
                [x["key"] for x in cm.need_refresh_items()],
                [
                    "671a8e4f0f414e06565dff65ec7e3a8e",
                    "3e474044f9e1663185a788015e91b325",
                ],
            )

        # smoke tests a few bits
        cm.refresh_cache(fn=fn)

        # test deleting keys
        cm.delete_expired()
        self.assertEqual(
            sorted(cm.keys()),
            [
                "3e474044f9e1663185a788015e91b325",
                "671a8e4f0f414e06565dff65ec7e3a8e",
            ],
        )

        cm.delete_cache("671a8e4f0f414e06565dff65ec7e3a8e")
        self.assertEqual(
            sorted(cm.keys()),
            [
                "3e474044f9e1663185a788015e91b325",
            ],
        )

        # delete an entry that doesn't exist...
        cm.delete_cache("00000000000000000000000000000000")
        self.assertEqual(
            sorted(cm.keys()),
            [
                "3e474044f9e1663185a788015e91b325",
            ],
        )

        cm.delete_all()
        self.assertEqual(sorted(cm.keys()), [])

    def test_memory_nodebug(self):
        self.test_memory(debug=False)

    def test_file(self):
        d1 = datetime.datetime(2017, 1, 1)

        dtmock = Mock(wraps=datetime.datetime)

        cm = CacheMockFile(timeout=60, grace=0)
        fn = cm(slow_function)

        # nothing in cache, should check cache and save value
        dtmock.utcnow = Mock(return_value=d1)
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo")
            self.assertEqual(val, [datetime.datetime(2017, 1, 1, 0, 0), 1, 2, "foo", 2])
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                    ["exists_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                    [
                        "update_cache",
                        "671a8e4f0f414e06565dff65ec7e3a8e",
                        {
                            "kwargs": {"k1": "foo"},
                            "args": (1, 2),
                            "value": val,
                            "key": "671a8e4f0f414e06565dff65ec7e3a8e",
                            "created": datetime.datetime(2017, 1, 1, 0, 0),
                        },
                    ],
                ],
            )
            self.assertEqual(sorted(cm.keys()), ["671a8e4f0f414e06565dff65ec7e3a8e"])

        # same function but different params, should also run function and store in cache
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=1))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="bar")
            self.assertEqual(
                val, [datetime.datetime(2017, 1, 1, 0, 0, 1), 1, 2, "bar", 2]
            )
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "3e474044f9e1663185a788015e91b325"],
                    ["exists_cache", "3e474044f9e1663185a788015e91b325"],
                    [
                        "update_cache",
                        "3e474044f9e1663185a788015e91b325",
                        {
                            "kwargs": {"k1": "bar"},
                            "args": (1, 2),
                            "value": val,
                            "key": "3e474044f9e1663185a788015e91b325",
                            "created": datetime.datetime(2017, 1, 1, 0, 0, 1),
                        },
                    ],
                ],
            )

        # back to first function, within cache timeout, should just return cached value
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=2))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo")
            self.assertEqual(val, ["2017-01-01T00:00:00", 1, 2, "foo", 2])
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                    ["exists_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                ],
            )

        # run with nocache, should skip caching stuff altogether, but not disrupt cache
        cm.log = []
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=3))
        with patch("cachelib.datetime.datetime", dtmock):
            val = fn(1, 2, k1="foo", _nocache=True)
            self.assertEqual(
                val, [datetime.datetime(2017, 1, 1, 0, 0, 3), 1, 2, "foo", 2]
            )
            self.assertEqual(cm.log, [])
            self.assertEqual(
                sorted(cm.keys()),
                [
                    "3e474044f9e1663185a788015e91b325",
                    "671a8e4f0f414e06565dff65ec7e3a8e",
                ],
            )

            val = fn(1, 2, k1="foo")
            self.assertEqual(val, ["2017-01-01T00:00:00", 1, 2, "foo", 2])
            self.assertEqual(
                cm.log,
                [
                    ["load_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                    ["exists_cache", "671a8e4f0f414e06565dff65ec7e3a8e"],
                ],
            )

        self.assertTrue(cm.exists_cache("3e474044f9e1663185a788015e91b325"))
        self.assertFalse(cm.exists_cache("00000000000000000000000000000000"))

        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=5))
        with patch("cachelib.datetime.datetime", dtmock):
            # smoke tests a few bits
            cm.refresh_cache(fn=fn)
            cm.expired_items()

        # test deleting keys
        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=5))
        with patch("cachelib.datetime.datetime", dtmock):
            cm.delete_expired()
            self.assertEqual(
                sorted(cm.keys()),
                [
                    "3e474044f9e1663185a788015e91b325",
                    "671a8e4f0f414e06565dff65ec7e3a8e",
                ],
            )

            cm.delete_cache("671a8e4f0f414e06565dff65ec7e3a8e")
            self.assertEqual(
                sorted(cm.keys()),
                [
                    "3e474044f9e1663185a788015e91b325",
                ],
            )

        dtmock.utcnow = Mock(return_value=d1 + datetime.timedelta(seconds=100))
        with patch("cachelib.datetime.datetime", dtmock):
            cm.delete_expired()
            self.assertEqual(sorted(cm.keys()), [])

        cm.delete_all()
        self.assertEqual(sorted(cm.keys()), [])


class CacheSmokeTest(TestCase):
    # just make secure we can instantiate these
    def test_smoke(self):
        session = boto3.session.Session(
            region_name="us-west-2",
        )
        s3 = session.resource("s3")
        cache.S3Cache("test_bucket", client=s3, prefix="foo", timeout=60)

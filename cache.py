import joblib
memory = joblib.Memory('/tmp/cprc/cache', verbose=1000)


def cache(func):
    return memory.cache(func)


def hash(obj):
    return joblib.hashing.hash(obj)

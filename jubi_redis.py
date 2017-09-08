import redis

class RedisPool:
    # 缓存连接，缓存可以随时flushdb
    conn = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, max_connections=10, decode_responses=True)
    # 存储连接，存储不能被flushdb
    rconn = redis.StrictRedis(host='127.0.0.1', port=6379, db=1, max_connections=10, decode_responses=True)
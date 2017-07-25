import redis

tickers_key = "tickers"


class RedisPool:
    conn = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, max_connections=10)
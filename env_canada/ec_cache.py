from aiohttp import ClientSession
from datetime import datetime, timedelta

CACHE_EXPIRE_TIME = timedelta(minutes=200)  # Time is tuned for 3h radar image


class CacheClientSession(ClientSession):
    """Shim to cache ClientSession requests."""

    _cache = {}

    def _flush_cache(self):
        """Flush expired cache entries."""

        now = datetime.now()
        expired = [key for key, value in self._cache.items() if value[0] < now]
        for key in expired:
            print(f"_flush_cache expiring {self._cache[key][0]} {key}")
            del self._cache[key]

    async def get(self, url, params, cache_time=CACHE_EXPIRE_TIME):
        """Thin wrapper around ClientSession.get to cache responses."""

        self._flush_cache()  # Flush at start so we don't use expired entries

        cache_key = (url, tuple(sorted(params.items())))
        result = self._cache.get(cache_key)
        if not result:
            result = (
                datetime.now() + cache_time,
                await super().get(url=url, params=params),
            )
            self._cache[cache_key] = result
            print(f"cached get NOT found!  {cache_key}")

        return result[1]

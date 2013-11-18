package com.qmetric.shiro.cache;

import net.spy.memcached.MemcachedClient;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class Memcached implements Cache<String, Object> {

    private static final Logger log = LoggerFactory.getLogger(Memcached.class);

    private MemcachedClient cache;

    public Memcached(MemcachedClient cache) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache argument cannot be null.");
        }
        this.cache = cache;
    }

    public Object get(String key) throws CacheException {
        try {
            if (log.isTraceEnabled()) {
                log.trace("Getting object from cache [" + cache + "] for key [" + key + "]");
            }
            if (key == null) {
                return null;
            } else {
                return cache.get(key.toString());
            }
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    public Object put(String key, Object value) throws CacheException {
        if (log.isTraceEnabled()) {
            log.trace("Putting object in cache [" + cache + "] for key [" + key + "]");
        }
        try {
            Object previous = get(key);
            cache.set(key.toString(), 0, value);
            return previous;
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    public Object remove(String key) throws CacheException {
        if (log.isTraceEnabled()) {
            log.trace("Removing object from cache [" + cache + "] for key [" + key + "]");
        }
        try {
            Object previous = get(key);
            cache.delete(key.toString());
            return previous;
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    public void clear() throws CacheException {
        if (log.isTraceEnabled()) {
            log.trace("Clearing all objects from cache [" + cache + "]");
        }
        try {
            cache.flush();
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    public int size() {
        return 0;
    }

    public Set<String> keys() {
        return Collections.emptySet();
    }

    public Collection<Object> values() {
        return Collections.emptyList();
    }

    public String toString() {
        return "Memcache [" + cache + "]";
    }
}

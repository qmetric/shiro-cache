package com.qmetric.shiro.cache;

import com.google.common.collect.Maps;
import net.spy.memcached.MemcachedClient;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.util.Destroyable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

@SuppressWarnings("unchecked")
public class MemcachedShiroCacheManager implements org.apache.shiro.cache.CacheManager, Destroyable {

    private Logger LOG = LoggerFactory.getLogger(MemcachedShiroCacheManager.class);

    private final String endpoint;

    private final int port;

    private final Map<String, Memcached> clients = Maps.newConcurrentMap();

    public MemcachedShiroCacheManager(final String endpoint, final int port) {
        this.endpoint = endpoint;
        this.port = port;
    }

    @Override
    public Cache getCache(String name) throws CacheException {
        LOG.debug(String.format("MemcachedShiroCacheManager.getCache(%s)", name));

        if (nameIsNotFound(name)) clients.put(name, new Memcached(createNewCacheInstance()));

        return clients.get(name);
    }

    private MemcachedClient createNewCacheInstance() {
        try {
            return new MemcachedClient(buildSocketConnection());
        } catch (IOException e) {
            throw new CacheException(e);
        }
    }

    private InetSocketAddress buildSocketConnection() {
        LOG.debug(String.format("endpoint: %s:%s", endpoint, port));

        return new InetSocketAddress(endpoint, port);
    }

    private boolean nameIsNotFound(String name) {
        return !clients.containsKey(name);
    }

    @Override
    public void destroy() throws Exception {
        clients.clear();
    }
}

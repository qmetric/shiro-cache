package com.qmetric.shiro.cache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.util.Destroyable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class MemcachedShiroCacheManager implements CacheManager, Destroyable {

    private static final Logger LOG = LoggerFactory.getLogger(MemcachedShiroCacheManager.class);

    private final Map<String, ShiroMemcached> clients = Maps.newConcurrentMap();

    private List<String> serverList;

    public MemcachedShiroCacheManager() {
    }

    public MemcachedShiroCacheManager(final String[] serverList) {
        setServerList(serverList);
    }

    @Override
    public Cache getCache(String name) throws CacheException {
        LOG.debug(String.format("MemcachedShiroCacheManager.getCache(%s)", name));

        if (nameIsNotFound(name)) try {
            clients.put(name, new ShiroMemcached(serverList));
        } catch (IOException e) {
            throw new CacheException(e);
        }

        return clients.get(name);
    }

    private boolean nameIsNotFound(String name) {
        return !clients.containsKey(name);
    }

    @Override
    public void destroy() throws Exception {
        clients.clear();
    }

    public void setServerList(final String[] serverList) {
        List<String> list = Lists.newArrayList();
        if (serverList != null) {
            for (String server : serverList) {
                list.add(server.replaceAll("\"", ""));
            }
        }
        this.serverList = list;
    }
}

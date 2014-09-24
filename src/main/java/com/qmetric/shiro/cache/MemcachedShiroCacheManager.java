package com.qmetric.shiro.cache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.util.Destroyable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@SuppressWarnings("unchecked")
public class MemcachedShiroCacheManager implements CacheManager, Destroyable {

    private static final Logger LOG = LoggerFactory.getLogger(MemcachedShiroCacheManager.class);

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private static final int DEFAULT_EXPIRATION_TIME_OF_20_MINUTES = 1200;

    private int expiryTime = DEFAULT_EXPIRATION_TIME_OF_20_MINUTES;

    private final Map<String, ShiroMemcached> clients = Maps.newConcurrentMap();

    private List<String> serverList;

    // do not remove this public constructor. it is used by clients that cannot perform constructor injections
    public MemcachedShiroCacheManager() {
    }

    public MemcachedShiroCacheManager(final String[] serverList) {
        setServerList(serverList);
    }

    public MemcachedShiroCacheManager(final String[] serverList, int expiryTime) {
        this(serverList);
        this.expiryTime = expiryTime;
    }

    @Override
    public Cache getCache(String name) throws CacheException {
        LOG.debug(String.format("MemcachedShiroCacheManager.getCache(%s)", name));

        if (nameIsNotFound(name)) {
            try {
                clients.put(name, new ShiroMemcached(serverList, expiryTime));
            } catch (IOException e) {
                throw new CacheException(e);
            }
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

    // do not remove this public method. it is used by clients that cannot perform constructor injections
    public void setServerList(final String[] serverList) {
        List<String> list = Lists.newArrayList();
        if (serverList != null) {
            for (String server : serverList) {
                list.add(server.replaceAll("\"", ""));
            }
        }
        this.serverList = list;
    }

    // do not remove this public method. it is used by clients that cannot perform constructor injections
    public void setExpiryTime(int time) {
        this.expiryTime = time;
    }

    public String healthCheck() {
        try {
            String value = UUID.randomUUID().toString();
            getCache("shiro-activeSessionCache").put("health-check-test", value);
            Object valueStored = getCache("shiro-activeSessionCache").remove("health-check-test");
            if (value.equals(valueStored)) {
                return toJson(new HealthCheckDetails(true, String.format("Memcached %s is healthy", serverList)));
            } else {
                return toJson(new HealthCheckDetails(false, String.format("Memcached %s is unhealthy, failed to find test value", serverList)));
            }
        } catch (CacheException e) {
            return toJson(new HealthCheckDetails(true, String.format("Memcached %s is unhealthy, %s", serverList, e.getMessage())));
        } finally {
            try {
                getCache("shiro-activeSessionCache").remove("health-check-test");
            } catch (CacheException e) {
                // too late anyway
            }
        }
    }

    public class HealthCheckDetails {
        public final boolean healthy;
        public final String message;

        public HealthCheckDetails(boolean healthy, String message) {
            this.healthy = healthy;
            this.message = message;
        }
    }

    private String toJson(HealthCheckDetails details) {
        return GSON.toJson(details);
    }
}
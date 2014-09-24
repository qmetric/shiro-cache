package com.qmetric.shiro.cache;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.spy.memcached.*;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static java.util.Arrays.asList;

//todo dfarr. this class is dump and not scalable. it also swallows cache exceptions so clients should be aware it can fail silently
public class ShiroMemcached implements Cache<String, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(ShiroMemcached.class);

    private static final int DEFAULT_EXPIRATION_TIME_OF_20_MINUTES = 1200;

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private int expiryTime = DEFAULT_EXPIRATION_TIME_OF_20_MINUTES;

    protected final List<MemcachedClient> clients;

    private List<String> serverList;

    public ShiroMemcached(List<String> serverList) throws IOException {
        clients = Lists.newArrayList();

        List<InetSocketAddress> addresses = AddrUtil.getAddresses(serverList);

        this.serverList = serverList;

        for (InetSocketAddress address : addresses) {
            try {
                ConnectionFactory connectionFactory = new ConnectionFactoryBuilder().setFailureMode(FailureMode.Retry).build();
                clients.add(new MemcachedClient(connectionFactory, asList(address)));
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", address), e);
            }
        }

        LOG.debug(String.format("Clients configured {%s}", clients));
    }

    protected ShiroMemcached(List<String> serverList, int expiryTime) throws IOException {
        this(serverList);
        this.expiryTime = expiryTime;
    }

    private int getExpirationTime() {
        return expiryTime;
    }

    public Object get(String key) throws CacheException {
        Object value = null;
        for (MemcachedClient client : clients) {
            try {
                value = client.get(key);
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", client.getVersions()), e);
            }
        }

        LOG.debug(String.format("Get {%s} returns {%s}", key, value));

        return value;
    }

    public Object put(String key, Object value) throws CacheException {
        Object previous = null;
        for (MemcachedClient client : clients) {
            try {
                previous = get(key);
                client.set(key, getExpirationTime(), value);
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", client.getVersions()), e);
            }
        }

        LOG.debug(String.format("Put {%s:%s} Previous returns {%s}", key, value, previous));

        return previous;
    }

    public Object remove(String key) throws CacheException {
        Object previous = null;
        for (MemcachedClient client : clients) {
            try {
                previous = get(key);
                client.delete(key);
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", client.getVersions()), e);
            }
        }

        LOG.debug(String.format("Remove {%s} Previous returns {%s}", key, previous));

        return previous;
    }

    public void clear() throws CacheException {

        for (MemcachedClient client : clients) {
            try {
                client.flush();
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", client.getVersions()), e);
            }
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

    public String healthCheck() {
        try {
            String value = UUID.randomUUID().toString();
            put("health-check-test", value);
            Object valueStored = remove("health-check-test");
            if (value.equals(valueStored)) {
                return toJson(new HealthCheckDetails(true, String.format("Memcached %s is healthy", serverList)));
            } else {
                return toJson(new HealthCheckDetails(false, String.format("Memcached %s is unhealthy, failed to find test value", serverList)));
            }
        } catch (CacheException e) {
            return toJson(new HealthCheckDetails(true, String.format("Memcached %s is unhealthy, %s", serverList, e.getMessage())));
        } finally {
            try {
                remove("health-check-test");
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

    private String toJson(HealthCheckDetails details)
    {
        return GSON.toJson(details);
    }
}

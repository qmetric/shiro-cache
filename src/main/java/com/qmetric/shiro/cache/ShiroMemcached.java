package com.qmetric.shiro.cache;

import com.google.common.collect.Lists;
import net.spy.memcached.*;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

//todo dfarr. this class is dump and not scalable. it also swallows cache exceptions so clients should be aware it can fail silently
public class ShiroMemcached implements Cache<String, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(ShiroMemcached.class);

    public static final int EXPIRE_IN_30_MINUTES = 1800;

    private final List<MemcachedClient> clients;

    public ShiroMemcached(List<String> serverList) throws IOException {
        clients = Lists.newArrayList();

        List<InetSocketAddress> addresses = AddrUtil.getAddresses(serverList);

        for (InetSocketAddress address : addresses) {
            try {
                ConnectionFactory connectionFactory = new ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build();
                clients.add(new MemcachedClient(connectionFactory, asList(address)));
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", address), e);
            }
        }

        LOG.debug(String.format("Clients configured {%s}", clients));

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
                client.set(key, EXPIRE_IN_30_MINUTES, value);
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
}

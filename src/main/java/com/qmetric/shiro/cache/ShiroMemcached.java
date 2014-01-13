package com.qmetric.shiro.cache;

import com.google.common.collect.Lists;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
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

//todo dfarr. this class is dump and not scalable. it also swallows cache exceptions so clients should be aware it can fail silently
public class ShiroMemcached implements Cache<String, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(ShiroMemcached.class);

    private final List<MemcachedClient> clients;

    public ShiroMemcached(List<String> serverList) {
        clients = Lists.newArrayList();

        List<InetSocketAddress> addresses = AddrUtil.getAddresses(serverList);

        for (InetSocketAddress address : addresses) {
            try {
                clients.add(new MemcachedClient(address));
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", address), e);
            }
        }
    }

    public Object get(String key) throws CacheException {
        Object value = null;
        for (MemcachedClient client : clients) {
            try {
                value = client.get(key);
                LOG.debug(String.format("Get {%s} from Client {%s} returns {%s}", key, client.getVersions(), value));
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", client.getVersions()), e);
            }
        }

        return value;

    }

    public Object put(String key, Object value) throws CacheException {
        Object previous = null;
        for (MemcachedClient client : clients) {
            try {
                previous = get(key);
                client.set(key, 0, value);
                LOG.debug(String.format("Put {%s:%s} to Client {%s} previous returns {%s}", key, value, client.getVersions(), previous));
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", client.getVersions()), e);
            }
        }

        return previous;
    }

    public Object remove(String key) throws CacheException {
        Object previous = null;
        for (MemcachedClient client : clients) {
            try {
                previous = get(key);
                client.delete(key);
                LOG.debug(String.format("Remove {%s} from Client {%s} previous returns {%s}", key, client.getVersions(), previous));
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", client.getVersions()), e);
            }
        }

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

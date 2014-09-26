package com.qmetric.shiro.cache;

import com.google.common.collect.Lists;
import net.spy.memcached.*;
import net.spy.memcached.internal.OperationFuture;
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

    private final List<MemcachedClient> clients;

    private final List<String> serverList;

    private final int expiryTime;

    public ShiroMemcached(List<String> serverList, int expiryTime) throws IOException {
        this.serverList = serverList;
        this.expiryTime = expiryTime;

        clients = buildMemcachdClients(serverList);

        LOG.debug(String.format("Clients configured {%s}", clients));
    }

    private List buildMemcachdClients(List<String> serverList) {
        List<MemcachedClient> clients = Lists.newArrayList();

        List<InetSocketAddress> addresses = AddrUtil.getAddresses(serverList);

        for (InetSocketAddress address : addresses) {
            try {
                ConnectionFactory connectionFactory = new ConnectionFactoryBuilder().setFailureMode(FailureMode.Retry).build();
                clients.add(new MemcachedClient(connectionFactory, asList(address)));
            } catch (Exception e) {
                LOG.error(String.format("client {%s} throw an exception", address), e);
                throw new CacheException(e);
            }
        }

        return clients;
    }

    private int getExpirationTime() {
        return expiryTime;
    }

    public Object get(String key) throws CacheException {
        Object value = null;

        LOG.debug(String.format("Start Thread {%s}", Thread.currentThread().getId()));
        int i = 0;
        for (MemcachedClient client : clients) {
            try {
                value = client.get(key);
                LOG.debug(String.format("Thread {%s} client {%s} key {%s} value {%s}", Thread.currentThread().getId(), serverList.get(i++), key, value));
            } catch (Exception e) {
                LOG.error(String.format("On get {%s}, client {%s} throw an exception", key, getServer(i)), e);
            }
        }

        LOG.debug(String.format("End Thread {%s}", Thread.currentThread().getId()));

        return value;
    }

    public Object put(String key, Object value) throws CacheException {
        Object previous = null;
        int i = 0;
        for (MemcachedClient client : clients) {
            try {
                previous = get(key);
                client.set(key, getExpirationTime(), value);
            } catch (Exception e) {
                LOG.error(String.format("On put {%s:%s}, client {%s} throw an exception", key, value, getServer(i)), e);
            }
        }

        LOG.debug(String.format("Put {%s:%s} Previous returns {%s}", key, value, previous));

        return previous;
    }

    public Object remove(String key) throws CacheException {
        Object previous = null;
        int i = 0;
        for (MemcachedClient client : clients) {
            try {
                previous = get(key);
                client.delete(key);
            } catch (Exception e) {
                LOG.error(String.format("On remove {%s}, client {%s} throw an exception", key, serverList.get(i++)), e);
            }
        }

        LOG.debug(String.format("Remove {%s} Previous returns {%s}", key, previous));

        return previous;
    }

    public void clear() throws CacheException {

        int i = 0;
        for (MemcachedClient client : clients) {
            try {
                OperationFuture<Boolean> flush = client.flush();
                flush.get();
            } catch (Exception e) {
                LOG.error(String.format("On clear, client {%s} throw an exception", getServer(i)), e);
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

    private String getServer(int i) {
        try {
            return serverList.get(i++);
        } catch (Exception e) {
            return "Unknown";
        }
    }
}

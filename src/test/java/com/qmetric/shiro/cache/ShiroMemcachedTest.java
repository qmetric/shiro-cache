package com.qmetric.shiro.cache;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class ShiroMemcachedTest {

    public static final List<String> SERVER_LIST = asList("memcached1.dev.qmetric.co.uk:11211", "memcached2.dev.qmetric.co.uk:11211");

    public static final int EXPIRY_TIME_OF_5_SECONDS = 5;

    public static final String TEST_KEY = "test-key";

    private ShiroMemcached shiroMemcached;

    @Before
    public void context() throws Exception {
        shiroMemcached = new ShiroMemcached(SERVER_LIST, EXPIRY_TIME_OF_5_SECONDS);
    }

    @Test
    public void getValue() throws IOException {
        shiroMemcached.put(TEST_KEY, "DOM");

        String actual = (String) shiroMemcached.get(TEST_KEY);

        assertThat(actual, equalTo("DOM"));
    }

    @Test
    public void noValue() throws IOException {
        String actual = (String) shiroMemcached.get(TEST_KEY);

        assertThat(actual, is(nullValue()));
    }

    @Test
    public void expiredValue() throws IOException, InterruptedException {
        shiroMemcached.put(TEST_KEY, "DOM");

        Thread.sleep(EXPIRY_TIME_OF_5_SECONDS * 1000 + 500);

        String actual = (String) shiroMemcached.get(TEST_KEY);

        assertThat(actual, is(nullValue()));
    }

    @After
    public void removeTestValue() {
        shiroMemcached.remove(TEST_KEY);
    }
}

/*
 * Copyright (c) 2014-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.folsom;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Utils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class KetamaIntegrationTest {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() throws Exception {
    ArrayList<Object[]> res = Lists.newArrayList();
    res.add(new Object[] {"ascii"});
    res.add(new Object[] {"binary"});
    return res;
  }

  @Parameterized.Parameter(0)
  public String protocol;

  private static Servers servers;

  private MemcacheClient<String> client;
  private int connections;

  @BeforeClass
  public static void setUpClass() throws Exception {
    servers = new Servers(3);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    servers.stop();
  }

  @Before
  public void setUp() throws Exception {
    connections = Utils.getGlobalConnectionCount();

    boolean ascii;
    if (protocol.equals("ascii")) {
      ascii = true;
    } else if (protocol.equals("binary")) {
      ascii = false;
    } else {
      throw new IllegalArgumentException(protocol);
    }
    MemcacheClientBuilder<String> builder =
        MemcacheClientBuilder.newStringClient()
            .withConnections(1)
            .withMaxOutstandingRequests(100)
            .withMetrics(NoopMetrics.INSTANCE)
            .withRetry(false)
            .withRequestTimeoutMillis(10 * 1000);
    for (MemcachedServer server : servers.servers) {
      builder.withAddress(server.getHost(), server.getPort());
    }

    if (ascii) {
      client = builder.connectAscii();
    } else {
      client = builder.connectBinary();
    }
    client.awaitFullyConnected(10, TimeUnit.SECONDS);
    System.out.println("Using client: " + client + ", protocol: " + protocol);
    servers.flush();
  }

  @After
  public void tearDown() throws Exception {
    client.shutdown();
    client.awaitDisconnected(10, TimeUnit.SECONDS);
    assertEquals(connections, Utils.getGlobalConnectionCount());
  }

  protected static final String KEY1 = "folsomtest:key1";
  protected static final String KEY2 = "folsomtest:key2";
  protected static final String KEY3 = "folsomtest:key3";
  protected static final String VALUE1 = "val1";
  protected static final String VALUE2 = "val2";
  protected static final String NUMERIC_VALUE = "123";

  public static final List<String> ALL_KEYS = ImmutableList.of(KEY1, KEY2, KEY3);

  protected static final int TTL = (int) Duration.ofMinutes(10).getSeconds();

  @Test
  public void testSetGet() throws Exception {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();

    assertEquals(VALUE1, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testLargeSet() throws Exception {
    String value = Collections.nCopies(10000, "Hello world ").toString();
    client.set(KEY1, value, TTL).toCompletableFuture().get();
    assertEquals(value, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testAppend() throws Exception {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    client.append(KEY1, VALUE2).toCompletableFuture().get();

    assertEquals(VALUE1 + VALUE2, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testAppendMissing() throws Throwable {
    IntegrationTest.checkStatus(client.append(KEY1, VALUE2), MemcacheStatus.ITEM_NOT_STORED);
  }

  @Test
  public void testPrependMissing() throws Throwable {
    IntegrationTest.checkStatus(client.prepend(KEY1, VALUE2), MemcacheStatus.ITEM_NOT_STORED);
  }

  @Test
  public void testPrepend() throws Exception {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    client.prepend(KEY1, VALUE2).toCompletableFuture().get();

    assertEquals(VALUE2 + VALUE1, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testAddGet() throws Exception {
    client.add(KEY1, VALUE1, TTL).toCompletableFuture().get();

    assertEquals(VALUE1, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testAddKeyExists() throws Throwable {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    IntegrationTest.checkStatus(client.add(KEY1, VALUE1, TTL), MemcacheStatus.ITEM_NOT_STORED);
  }

  @Test
  public void testReplaceGet() throws Exception {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();

    client.replace(KEY1, VALUE2, TTL).toCompletableFuture().get();

    assertEquals(VALUE2, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testMultiGetAllKeysExistsIterable() throws Throwable {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    client.set(KEY2, VALUE2, TTL).toCompletableFuture().get();

    assertEquals(
        asList(VALUE1, VALUE2), client.get(asList(KEY1, KEY2)).toCompletableFuture().get());
  }

  @Test
  public void testDeleteKeyNotExist() throws Throwable {
    IntegrationTest.checkKeyNotFound(client.delete(KEY1));
  }

  @Test
  public void testMultiget() throws Exception {
    for (int i = 0; i < 100; i++) {
      client.set("key-" + i, "value-" + i, 0).toCompletableFuture().get();
    }
    for (int i = 0; i < 100; i++) {
      assertEquals("value-" + i, client.get("key-" + i).toCompletableFuture().get());
    }
    int listSize = 10;
    for (int i = 0; i <= 100 - listSize; i++) {
      List<String> keys = Lists.newArrayList();
      List<String> expectedValues = Lists.newArrayList();
      for (int j = 0; j < listSize; j++) {
        keys.add("key-" + (i + j));
        expectedValues.add("value-" + (i + j));
      }
      assertEquals(expectedValues, client.get(keys).toCompletableFuture().get());
    }
    // TODO: count number of keys per server
  }

  @Test
  public void testFlush() throws Throwable {
    for (int i = 0; i < 100; i++) {
      client.set("key-" + i, "value-" + i, 0).toCompletableFuture().get();
    }
    for (int i = 0; i < 100; i++) {
      assertEquals("value-" + i, client.get("key-" + i).toCompletableFuture().get());
    }
    client.flushAll(0).toCompletableFuture().get();
    for (int i = 0; i < 100; i++) {
      assertEquals(null, client.get("key-" + i).toCompletableFuture().get(1, TimeUnit.SECONDS));
    }
  }

  public static class Servers {
    private final List<MemcachedServer> servers;

    public Servers(int instances) {
      servers = Lists.newArrayList();
      for (int i = 0; i < instances; i++) {
        servers.add(new MemcachedServer());
      }
    }

    public void stop() {
      for (MemcachedServer server : servers) {
        server.stop();
      }
    }

    public MemcachedServer getInstance(int i) {
      return servers.get(i);
    }

    List<MemcachedServer> getServers() {
      return servers;
    }

    void flush() {
      servers.forEach(MemcachedServer::flush);
    }
  }
}

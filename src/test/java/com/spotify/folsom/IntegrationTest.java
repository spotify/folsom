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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Utils;
import junit.framework.AssertionFailedError;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class IntegrationTest {

  private static final HostAndPort DEFAULT_SERVER_ADDRESS
          = HostAndPort.fromParts("127.0.0.1", 11211);
  private static EmbeddedServer asciiEmbeddedServer;
  private static EmbeddedServer binaryEmbeddedServer;

  private static HostAndPort getServerAddress() {
    String addressOverride = System.getProperty("memcached-address");
    if (addressOverride != null) {
      return HostAndPort.fromString(addressOverride).withDefaultPort(11211);
    } else {
      return DEFAULT_SERVER_ADDRESS;
    }
  }

  private static boolean memcachedRunning() {
    try (Socket socket = new Socket()) {
      HostAndPort serverAddress = getServerAddress();
      socket.connect(new InetSocketAddress(
          HostAndPortFix.getHostText(serverAddress),
          serverAddress.getPort()));
      return true;
    } catch (ConnectException e) {
      System.err.println("memcached not running, disabling test");
      return false;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Parameterized.Parameters(name = "{0}-{1}")
  public static Collection<Object[]> data() throws Exception {
    ArrayList<Object[]> res = Lists.newArrayList();
    res.add(new Object[]{"embedded", "ascii"});
    res.add(new Object[]{"embedded", "binary"});
    if (memcachedRunning()) {
      res.add(new Object[]{"integration", "ascii"});
      res.add(new Object[]{"integration", "binary"});
    }
    return res;
  }

  @Parameterized.Parameter(0)
  public String type;
  @Parameterized.Parameter(1)
  public String protocol;

  private MemcacheClient<String> client;
  private AsciiMemcacheClient<String> asciiClient;
  private BinaryMemcacheClient<String> binaryClient;

  @BeforeClass
  public static void setUpClass() throws Exception {
    asciiEmbeddedServer = new EmbeddedServer(false);
    binaryEmbeddedServer = new EmbeddedServer(true);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    binaryEmbeddedServer.stop();
    asciiEmbeddedServer.stop();
  }

  @Before
  public void setUp() throws Exception {
    assertEquals(0, Utils.getGlobalConnectionCount());

    boolean ascii;
    if (protocol.equals("ascii")) {
      ascii = true;
    } else if (protocol.equals("binary")) {
      ascii = false;
    } else {
      throw new IllegalArgumentException(protocol);
    }
    HostAndPort integrationServer = getServerAddress();

    int embeddedPort = ascii ? asciiEmbeddedServer.getPort() : binaryEmbeddedServer.getPort();
    String address = isEmbedded() ? "127.0.0.1" : HostAndPortFix.getHostText(integrationServer);
    int port = isEmbedded() ? embeddedPort : integrationServer.getPort();

    MemcacheClientBuilder<String> builder = MemcacheClientBuilder.newStringClient()
            .withAddress(HostAndPort.fromParts(address, port))
            .withConnections(1)
            .withMaxOutstandingRequests(1000)
            .withMetrics(NoopMetrics.INSTANCE)
            .withRetry(false)
            .withReplyExecutor(Utils.SAME_THREAD_EXECUTOR)
            .withRequestTimeoutMillis(100);

    if (ascii) {
      asciiClient = builder.connectAscii();
      binaryClient = null;
      client = asciiClient;
    } else {
      binaryClient = builder.connectBinary();
      asciiClient = null;
      client = binaryClient;
    }
    ConnectFuture.connectFuture(client).get();
    System.out.printf("Using client: %s protocol: %s and port: %d\n", client, protocol, port);
    cleanup();
  }

  private void cleanup() throws ExecutionException, InterruptedException {
    for (String key : ALL_KEYS) {
      client.delete(key).get();
    }
  }

  @After
  public void tearDown() throws Exception {
    cleanup();
    client.shutdown();
    ConnectFuture.disconnectFuture(client).get();

    assertEquals(0, Utils.getGlobalConnectionCount());
  }

  protected static final String KEY1 = "folsomtest:key1";
  protected static final String KEY2 = "folsomtest:key2";
  protected static final String KEY3 = "folsomtest:key3";
  protected static final String KEY4 = "folsomtest:keywithÅÄÖ";
  protected static final String VALUE1 = "val1";
  protected static final String VALUE2 = "val2";
  protected static final String NUMERIC_VALUE = "123";

  public static final List<String> ALL_KEYS = ImmutableList.of(KEY1, KEY2, KEY3, KEY4);

  protected static final int TTL = Integer.MAX_VALUE;

  @Test
  public void testSetGet() throws Exception {
    client.set(KEY1, VALUE1, TTL).get();

    assertEquals(VALUE1, client.get(KEY1).get());
  }

  @Test
  public void testSetGetWithUTF8() throws Exception {
    client.set(KEY4, VALUE1, TTL).get();

    assertEquals(VALUE1, client.get(KEY4).get());
  }

  @Test
  public void testLargeSet() throws Exception {
    String value = createValue(100000);
    client.set(KEY1, value, TTL).get();
    assertEquals(value, client.get(KEY1).get());
  }

  @Test
  public void testAppend() throws Exception {
    client.set(KEY1, VALUE1, TTL).get();
    client.append(KEY1, VALUE2).get();

    assertEquals(VALUE1 + VALUE2, client.get(KEY1).get());
  }

  @Test
  public void testAppendMissing() throws Throwable {
    checkStatus(client.append(KEY1, VALUE2), MemcacheStatus.ITEM_NOT_STORED);
  }

  @Test
  public void testPrependMissing() throws Throwable {
    checkStatus(client.prepend(KEY1, VALUE2), MemcacheStatus.ITEM_NOT_STORED);
  }

  @Test
  public void testAppendCas() throws Exception {
    assumeBinary();

    binaryClient.set(KEY1, VALUE1, TTL).get();
    final long cas = binaryClient.casGet(KEY1).get().getCas();
    binaryClient.append(KEY1, VALUE2, cas).get();

    assertEquals(VALUE1 + VALUE2, binaryClient.get(KEY1).get());
  }

  @Test
  public void testAppendCasIncorrect() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("CAS is broken on embedded server");

    binaryClient.set(KEY1, VALUE1, TTL).get();
    final long cas = binaryClient.casGet(KEY1).get().getCas();
    checkStatus(binaryClient.append(KEY1, VALUE2, cas + 666), MemcacheStatus.KEY_EXISTS);
  }

  @Test
  public void testPrependCasIncorrect() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("CAS is broken on embedded server");

    binaryClient.set(KEY1, VALUE1, TTL).get();
    final long cas = binaryClient.casGet(KEY1).get().getCas();
    checkStatus(binaryClient.prepend(KEY1, VALUE2, cas + 666), MemcacheStatus.KEY_EXISTS);
  }

  @Test
  public void testPrependCas() throws Exception {
    assumeBinary();

    binaryClient.set(KEY1, VALUE1, TTL).get();
    final long cas = binaryClient.casGet(KEY1).get().getCas();
    binaryClient.prepend(KEY1, VALUE2, cas).get();

    assertEquals(VALUE2 + VALUE1, binaryClient.get(KEY1).get());
  }

  @Test
  public void testPrepend() throws Exception {
    client.set(KEY1, VALUE1, TTL).get();
    client.prepend(KEY1, VALUE2).get();

    assertEquals(VALUE2 + VALUE1, client.get(KEY1).get());
  }

  @Test
  public void testSetDeleteGet() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("returns \"\" instead of NOT_FOUND on embedded server");

    client.set(KEY1, VALUE1, TTL).get();

    client.delete(KEY1).get();

    assertGetKeyNotFound(client.get(KEY1));
  }

  @Test
  public void testAddGet() throws Exception {
    client.add(KEY1, VALUE1, TTL).get();

    assertEquals(VALUE1, client.get(KEY1).get());
  }

  @Test
  public void testAddKeyExists() throws Throwable {
    client.set(KEY1, VALUE1, TTL).get();
    checkStatus(client.add(KEY1, VALUE1, TTL), MemcacheStatus.ITEM_NOT_STORED);
  }

  @Test
  public void testReplaceGet() throws Exception {
    client.set(KEY1, VALUE1, TTL).get();

    client.replace(KEY1, VALUE2, TTL).get();

    assertEquals(VALUE2, client.get(KEY1).get());
  }

  @Test
  public void testGetKeyNotExist() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("returns \"\" instead of NOT_FOUND on embedded server");

    assertGetKeyNotFound(client.get(KEY1));
  }

  @Test
  public void testPartitionMultiget() throws Exception {
    final List<String> keys = Lists.newArrayList();
    for (int i = 0; i < 500; i++) {
      keys.add("key-" + i);
    }

    final List<ListenableFuture<MemcacheStatus>> futures = Lists.newArrayList();
    try {
      for (final String key : keys) {
        futures.add(client.set(key, key, TTL));
      }

      Futures.allAsList(futures).get();

      final ListenableFuture<List<String>> resultsFuture = client.get(keys);
      final List<String> results = resultsFuture.get();
      assertEquals(keys, results);
    } finally {
      futures.clear();
      for (final String key : keys) {
        futures.add(client.delete(key));
      }
      Futures.allAsList(futures).get();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTooLongKey() throws Exception {
    client.get(String.format("key-%300d", 1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidKey() throws Exception {
    client.get("key with spaces");
  }

  @Test
  public void testMultiGetAllKeysExistsIterable() throws Throwable {
    client.set(KEY1, VALUE1, TTL).get();
    client.set(KEY2, VALUE2, TTL).get();

    assertEquals(asList(VALUE1, VALUE2), client.get(asList(KEY1, KEY2)).get());
  }

  @Test
  public void testMultiGetWithCas() throws Throwable {
    client.set(KEY1, VALUE1, TTL).get();
    client.set(KEY2, VALUE2, TTL).get();

    long cas1 = client.casGet(KEY1).get().getCas();
    long cas2 = client.casGet(KEY2).get().getCas();

    List<GetResult<String>> expected = asList(
            GetResult.success(VALUE1, cas1),
            GetResult.success(VALUE2, cas2));
    assertEquals(expected, client.casGet(asList(KEY1, KEY2)).get());
  }

  @Test
  public void testMultiGetKeyMissing() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("returns \"\" instead of NOT_FOUND on embedded server");

    client.set(KEY1, VALUE1, TTL).get();
    client.set(KEY2, VALUE2, TTL).get();

    assertEquals(asList(VALUE1, null, VALUE2), client.get(Arrays.asList(KEY1, KEY3, KEY2)).get());
  }

  @Test
  public void testDeleteKeyNotExist() throws Throwable {
    checkKeyNotFound(client.delete(KEY1));
  }

  @Test
  public void testIncr() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("INCR/DECR gives NPE on embedded server");

    assertEquals(new Long(3), binaryClient.incr(KEY1, 2, 3, TTL).get());
    assertEquals(new Long(5), binaryClient.incr(KEY1, 2, 7, TTL).get());
  }

  private boolean isEmbedded() {
    return type.equals("embedded");
  }

  private boolean isAscii() {
    return protocol.equals("ascii");
  }

  private boolean isBinary() {
    return protocol.equals("binary");
  }

  @Test
  public void testDecr() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("INCR/DECR gives NPE on embedded server");

    assertEquals(new Long(3), binaryClient.decr(KEY1, 2, 3, TTL).get());
    assertEquals(new Long(1), binaryClient.decr(KEY1, 2, 5, TTL).get());
  }

  @Test
  public void testDecrBelowZero() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("INCR/DECR gives NPE on embedded server");

    assertEquals(new Long(3), binaryClient.decr(KEY1, 2, 3, TTL).get());
    // should not go below 0
    assertEquals(new Long(0), binaryClient.decr(KEY1, 4, 5, TTL).get());
  }

  @Test
  public void testIncrDefaults() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("INCR/DECR gives NPE on embedded server");

    // Ascii client behaves differently for this use case
    assertEquals(new Long(0), binaryClient.incr(KEY1, 2, 0, 0).get());
    // memcached will intermittently cause this test to fail due to the value not being set
    // correctly when incr with initial=0 is used, this works around that
    binaryClient.set(KEY1, "0", TTL).get();
    assertEquals(new Long(2), binaryClient.incr(KEY1, 2, 0, 0).get());
  }

  @Test
  public void testIncrDefaultsAscii() throws Throwable {
    assumeAscii();

    // Ascii client behaves differently for this use case
    assertEquals(null, asciiClient.incr(KEY1, 2).get());
    // memcached will intermittently cause this test to fail due to the value not being set
    // correctly when incr with initial=0 is used, this works around that
    asciiClient.set(KEY1, "0", TTL).get();
    assertEquals(new Long(2), asciiClient.incr(KEY1, 2).get());
  }

  @Test
  public void testIncrAscii() throws Throwable {
    assumeAscii();

    asciiClient.set(KEY1, NUMERIC_VALUE, TTL).get();
    assertEquals(new Long(125), asciiClient.incr(KEY1, 2).get());
  }

  @Test
  public void testDecrAscii() throws Throwable {
    assumeAscii();

    asciiClient.set(KEY1, NUMERIC_VALUE, TTL).get();
    assertEquals(new Long(121), asciiClient.decr(KEY1, 2).get());
  }


  @Test
  public void testCas() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("CAS is broken on embedded server");

    client.set(KEY1, VALUE1, TTL).get();

    final GetResult<String> getResult1 = client.casGet(KEY1).get();
    assertNotNull(getResult1.getCas());
    assertEquals(VALUE1, getResult1.getValue());

    client.set(KEY1, VALUE2, TTL, getResult1.getCas()).get();
    final long newCas = client.casGet(KEY1).get().getCas();


    assertNotEquals(0, newCas);

    final GetResult<String> getResult2 = client.casGet(KEY1).get();

    assertEquals(newCas, getResult2.getCas());
    assertEquals(VALUE2, getResult2.getValue());
  }

  @Test
  public void testCasNotMatchingSet() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("CAS is broken on embedded server");

    client.set(KEY1, VALUE1, TTL).get();

    checkStatus(client.set(KEY1, VALUE2, TTL, 666), MemcacheStatus.KEY_EXISTS);
  }

  @Test
  public void testTouchNotFound() throws Throwable {
    assumeNotEmbedded("touch is broken on embedded server");

    MemcacheStatus result = client.touch(KEY1, TTL).get();
    assertEquals(MemcacheStatus.KEY_NOT_FOUND, result);
  }

  @Test
  public void testTouchFound() throws Throwable {
    assumeNotEmbedded("touch is broken on embedded server");

    client.set(KEY1, VALUE1, TTL).get();
    MemcacheStatus result = client.touch(KEY1, TTL).get();
    assertEquals(MemcacheStatus.OK, result);
  }

  @Test
  public void testTouchAndGetFound() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("touch is broken on embedded server");

    client.set(KEY1, VALUE1, TTL).get();
    assertEquals(VALUE1, binaryClient.getAndTouch(KEY1, TTL).get());
  }

  @Test
  public void testTouchAndGetNotFound() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("touch is broken on embedded server");

    assertNull(binaryClient.getAndTouch(KEY1, TTL).get());
  }

  @Test
  public void testCasTouchAndGetFound() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("touch is broken on embedded server");

    client.set(KEY1, VALUE1, TTL).get();
    long cas = client.casGet(KEY1).get().getCas();
    GetResult<String> result = binaryClient.casGetAndTouch(KEY1, TTL).get();
    assertEquals(cas, result.getCas());
    assertEquals(VALUE1, result.getValue());
  }

  @Test
  public void testCasTouchAndGetNotFound() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("touch is broken on embedded server");

    GetResult<String> result = binaryClient.casGetAndTouch(KEY1, TTL).get();
    assertNull(result);
  }

  @Test
  public void testNoop() throws Throwable {
    assumeBinary();
    assumeNotEmbedded("touch is broken on embedded server");

    binaryClient.noop().get();
  }

  @Test
  public void testAlmostTooLargeValues() throws Exception {
    String value = createValue(1024 * 1024 - 1000);
    assertEquals(MemcacheStatus.OK, client.set(KEY1, value, TTL).get());
    assertEquals(value, client.get(KEY1).get());
  }

  @Test
  public void testTooLargeValues() throws Exception {
    String value = createValue(2 * 1024 * 1024);
    assertEquals(MemcacheStatus.OK, client.set(KEY1, "", TTL).get());
    assertEquals(MemcacheStatus.VALUE_TOO_LARGE, client.set(KEY1, value, TTL).get());
    assertEquals("", client.get(KEY1).get());
  }

  @Test
  public void testTooLargeValueDoesNotCorruptConnection() throws Exception {
    String largeValue = createValue(2 * 1024 * 1024);
    client.set(KEY1, largeValue, TTL).get();

    String smallValue = createValue(1000);
    for (int i = 0; i < 1000; i++) {
      assertEquals(MemcacheStatus.OK, client.set(KEY1, smallValue, TTL).get());
      assertEquals(smallValue, client.get(KEY1).get());
    }
  }

  private String createValue(int size) {
    StringBuilder sb = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      sb.append('.');
    }
    return sb.toString();
  }

  private void assumeNotEmbedded(String msg) {
    assumeFalse(msg, isEmbedded());
  }

  private void assumeAscii() {
    assumeTrue(isAscii());
  }

  private void assumeBinary() {
    assumeTrue(isBinary());
  }

  static void assertGetKeyNotFound(ListenableFuture<String> future) throws Throwable {
    checkKeyNotFound(future);
  }

  static void checkKeyNotFound(final ListenableFuture<?> future) throws Throwable {
    checkStatus(future, MemcacheStatus.KEY_NOT_FOUND);
  }

  static void checkStatus(final ListenableFuture<?> future, final MemcacheStatus... expected)
      throws Throwable {
    final Set<MemcacheStatus> expectedSet = Sets.newHashSet(expected);
    try {
      Object v = future.get();
      if (v instanceof MemcacheStatus) {
        final MemcacheStatus status = (MemcacheStatus) v;
        if (!expectedSet.contains(status)) {
          throw new AssertionFailedError(status + " not in " + expectedSet);
        }
      } else {
        if (v == null && expectedSet.contains(MemcacheStatus.KEY_NOT_FOUND)) {
          // ok
        } else if (v != null && expectedSet.contains(MemcacheStatus.OK)) {
          // ok
        } else {
          throw new AssertionFailedError();
        }
      }
    } catch (final ExecutionException e) {
      throw e.getCause();
    }
  }
}

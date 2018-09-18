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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.futures.CompletableFutures;
import java.util.concurrent.CompletionStage;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Utils;
import java.util.concurrent.TimeUnit;
import junit.framework.AssertionFailedError;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class IntegrationTest {

  private static final HostAndPort DEFAULT_SERVER_ADDRESS
          = HostAndPort.fromParts("127.0.0.1", 11211);
  private static MemcachedServer server;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() throws Exception {
    ArrayList<Object[]> res = Lists.newArrayList();
    res.add(new Object[]{"ascii"});
    res.add(new Object[]{"binary"});
    return res;
  }

  @Parameterized.Parameter(0)
  public String protocol;

  private MemcacheClient<String> client;
  private AsciiMemcacheClient<String> asciiClient;
  private BinaryMemcacheClient<String> binaryClient;

  @BeforeClass
  public static void setUpClass() throws Exception {
    server = new MemcachedServer();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    server.stop();
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
    HostAndPort integrationServer = HostAndPort.fromParts(server.getHost(), server.getPort());

    MemcacheClientBuilder<String> builder = MemcacheClientBuilder.newStringClient()
            .withAddress(server.getHost(), server.getPort())
            .withConnections(1)
            .withMaxOutstandingRequests(1000)
            .withMetrics(NoopMetrics.INSTANCE)
            .withRetry(false)
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
    client.awaitConnected(10, TimeUnit.SECONDS);
    System.out.printf("Using client: %s protocol: %s and port: %d\n",
        client, protocol, server.getPort());
    cleanup();
  }

  private void cleanup() throws ExecutionException, InterruptedException {
    for (String key : ALL_KEYS) {
      client.delete(key).toCompletableFuture().get();
    }
  }

  @After
  public void tearDown() throws Exception {
    cleanup();
    client.shutdown();
    client.awaitDisconnected(10, TimeUnit.SECONDS);

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
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();

    assertEquals(VALUE1, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testSetGetWithUTF8() throws Exception {
    client.set(KEY4, VALUE1, TTL).toCompletableFuture().get();

    assertEquals(VALUE1, client.get(KEY4).toCompletableFuture().get());
  }

  @Test
  public void testLargeSet() throws Exception {
    String value = createValue(100000);
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
    checkStatus(client.append(KEY1, VALUE2), MemcacheStatus.ITEM_NOT_STORED);
  }

  @Test
  public void testPrependMissing() throws Throwable {
    checkStatus(client.prepend(KEY1, VALUE2), MemcacheStatus.ITEM_NOT_STORED);
  }

  @Test
  public void testAppendCas() throws Exception {
    assumeBinary();

    binaryClient.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    final long cas = binaryClient.casGet(KEY1).toCompletableFuture().get().getCas();
    binaryClient.append(KEY1, VALUE2, cas).toCompletableFuture().get();

    assertEquals(VALUE1 + VALUE2, binaryClient.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testAppendCasIncorrect() throws Throwable {
    assumeBinary();

    binaryClient.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    final long cas = binaryClient.casGet(KEY1).toCompletableFuture().get().getCas();
    checkStatus(binaryClient.append(KEY1, VALUE2, cas + 666), MemcacheStatus.KEY_EXISTS);
  }

  @Test
  public void testPrependCasIncorrect() throws Throwable {
    assumeBinary();

    binaryClient.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    final long cas = binaryClient.casGet(KEY1).toCompletableFuture().get().getCas();
    checkStatus(binaryClient.prepend(KEY1, VALUE2, cas + 666), MemcacheStatus.KEY_EXISTS);
  }

  @Test
  public void testPrependCas() throws Exception {
    assumeBinary();

    binaryClient.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    final long cas = binaryClient.casGet(KEY1).toCompletableFuture().get().getCas();
    binaryClient.prepend(KEY1, VALUE2, cas).toCompletableFuture().get();

    assertEquals(VALUE2 + VALUE1, binaryClient.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testPrepend() throws Exception {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    client.prepend(KEY1, VALUE2).toCompletableFuture().get();

    assertEquals(VALUE2 + VALUE1, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testSetDeleteGet() throws Throwable {
    assumeBinary();

    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();

    client.delete(KEY1).toCompletableFuture().get();

    assertGetKeyNotFound(client.get(KEY1));
  }

  @Test
  public void testAddGet() throws Exception {
    client.add(KEY1, VALUE1, TTL).toCompletableFuture().get();

    assertEquals(VALUE1, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testAddKeyExists() throws Throwable {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    checkStatus(client.add(KEY1, VALUE1, TTL), MemcacheStatus.ITEM_NOT_STORED);
  }

  @Test
  public void testReplaceGet() throws Exception {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();

    client.replace(KEY1, VALUE2, TTL).toCompletableFuture().get();

    assertEquals(VALUE2, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testGetKeyNotExist() throws Throwable {
    assumeBinary();

    assertGetKeyNotFound(client.get(KEY1));
  }

  @Test
  public void testBinaryFlush() throws Throwable {
    assumeBinary();
    client.set(KEY1, "value1", 10000);
    client.set(KEY2, "value2", 10000);
    assertEquals("value1", client.get(KEY1).toCompletableFuture().get(1, TimeUnit.SECONDS));
    client.flushAll(0).toCompletableFuture().get();
    assertGetKeyNotFound(client.get(KEY1));
    assertGetKeyNotFound(client.get(KEY2));
  }

  @Test
  public void testAsciiFlush() throws Throwable {
    assumeAscii();
    client.set(KEY1, "value1", 10000);
    client.set(KEY2, "value2", 10000);
    assertEquals("value1", client.get(KEY1).toCompletableFuture().get(1, TimeUnit.SECONDS));
    client.flushAll(0).toCompletableFuture().get();
    assertGetKeyNotFound(client.get(KEY1));
    assertGetKeyNotFound(client.get(KEY2));
  }

  @Test
  public void testPartitionMultiget() throws Exception {
    final List<String> keys = Lists.newArrayList();
    for (int i = 0; i < 500; i++) {
      keys.add("key-" + i);
    }

    final List<CompletionStage<MemcacheStatus>> futures = Lists.newArrayList();
    try {
      for (final String key : keys) {
        futures.add(client.set(key, key, TTL));
      }

      CompletableFutures.allAsList(futures).get();

      final CompletionStage<List<String>> resultsFuture = client.get(keys);
      final List<String> results = resultsFuture.toCompletableFuture().get();
      assertEquals(keys, results);
    } finally {
      futures.clear();
      for (final String key : keys) {
        futures.add(client.delete(key));
      }
      CompletableFutures.allAsList(futures).get();
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
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    client.set(KEY2, VALUE2, TTL).toCompletableFuture().get();

    assertEquals(
        asList(VALUE1, VALUE2),
        client.get(asList(KEY1, KEY2)).toCompletableFuture().get());
  }

  @Test
  public void testMultiGetWithCas() throws Throwable {
    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    client.set(KEY2, VALUE2, TTL).toCompletableFuture().get();

    long cas1 = client.casGet(KEY1).toCompletableFuture().get().getCas();
    long cas2 = client.casGet(KEY2).toCompletableFuture().get().getCas();

    List<GetResult<String>> expected = asList(
            GetResult.success(VALUE1, cas1),
            GetResult.success(VALUE2, cas2));
    assertEquals(expected, client.casGet(asList(KEY1, KEY2)).toCompletableFuture().get());
  }

  @Test
  public void testMultiGetKeyMissing() throws Throwable {
    assumeBinary();

    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    client.set(KEY2, VALUE2, TTL).toCompletableFuture().get();

    assertEquals(
        asList(VALUE1, null, VALUE2),
        client.get(Arrays.asList(KEY1, KEY3, KEY2)).toCompletableFuture().get());
  }

  @Test
  public void testDeleteKeyNotExist() throws Throwable {
    checkKeyNotFound(client.delete(KEY1));
  }

  @Test
  public void testIncr() throws Throwable {
    assumeBinary();

    assertEquals(new Long(3), binaryClient.incr(KEY1, 2, 3, TTL).toCompletableFuture().get());
    assertEquals(new Long(5), binaryClient.incr(KEY1, 2, 7, TTL).toCompletableFuture().get());
  }

  private boolean isEmbedded() {
    return false;
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

    assertEquals(new Long(3), binaryClient.decr(KEY1, 2, 3, TTL).toCompletableFuture().get());
    assertEquals(new Long(1), binaryClient.decr(KEY1, 2, 5, TTL).toCompletableFuture().get());
  }

  @Test
  public void testDecrBelowZero() throws Throwable {
    assumeBinary();

    assertEquals(new Long(3), binaryClient.decr(KEY1, 2, 3, TTL).toCompletableFuture().get());
    // should not go below 0
    assertEquals(new Long(0), binaryClient.decr(KEY1, 4, 5, TTL).toCompletableFuture().get());
  }

  @Test
  public void testIncrDefaults() throws Throwable {
    assumeBinary();

    // Ascii client behaves differently for this use case
    assertEquals(new Long(0), binaryClient.incr(KEY1, 2, 0, 0).toCompletableFuture().get());
    // memcached will intermittently cause this test to fail due to the value not being set
    // correctly when incr with initial=0 is used, this works around that
    binaryClient.set(KEY1, "0", TTL).toCompletableFuture().get();
    assertEquals(new Long(2), binaryClient.incr(KEY1, 2, 0, 0).toCompletableFuture().get());
  }

  @Test
  public void testIncrDefaultsAscii() throws Throwable {
    assumeAscii();

    // Ascii client behaves differently for this use case
    assertEquals(null, asciiClient.incr(KEY1, 2).toCompletableFuture().get());
    // memcached will intermittently cause this test to fail due to the value not being set
    // correctly when incr with initial=0 is used, this works around that
    asciiClient.set(KEY1, "0", TTL).toCompletableFuture().get();
    assertEquals(new Long(2), asciiClient.incr(KEY1, 2).toCompletableFuture().get());
  }

  @Test
  public void testIncrAscii() throws Throwable {
    assumeAscii();

    asciiClient.set(KEY1, NUMERIC_VALUE, TTL).toCompletableFuture().get();
    assertEquals(new Long(125), asciiClient.incr(KEY1, 2).toCompletableFuture().get());
  }

  @Test
  public void testDecrAscii() throws Throwable {
    assumeAscii();

    asciiClient.set(KEY1, NUMERIC_VALUE, TTL).toCompletableFuture().get();
    assertEquals(new Long(121), asciiClient.decr(KEY1, 2).toCompletableFuture().get());
  }


  @Test
  public void testCas() throws Throwable {
    assumeBinary();

    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();

    final GetResult<String> getResult1 = client.casGet(KEY1).toCompletableFuture().get();
    assertNotNull(getResult1.getCas());
    assertEquals(VALUE1, getResult1.getValue());

    client.set(KEY1, VALUE2, TTL, getResult1.getCas()).toCompletableFuture().get();
    final long newCas = client.casGet(KEY1).toCompletableFuture().get().getCas();


    assertNotEquals(0, newCas);

    final GetResult<String> getResult2 = client.casGet(KEY1).toCompletableFuture().get();

    assertEquals(newCas, getResult2.getCas());
    assertEquals(VALUE2, getResult2.getValue());
  }

  @Test
  public void testCasNotMatchingSet() throws Throwable {
    assumeBinary();

    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();

    checkStatus(client.set(KEY1, VALUE2, TTL, 666), MemcacheStatus.KEY_EXISTS);
  }

  @Test
  public void testTouchNotFound() throws Throwable {

    MemcacheStatus result = client.touch(KEY1, TTL).toCompletableFuture().get();
    assertEquals(MemcacheStatus.KEY_NOT_FOUND, result);
  }

  @Test
  public void testTouchFound() throws Throwable {

    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    MemcacheStatus result = client.touch(KEY1, TTL).toCompletableFuture().get();
    assertEquals(MemcacheStatus.OK, result);
  }

  @Test
  public void testTouchAndGetFound() throws Throwable {
    assumeBinary();

    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    assertEquals(VALUE1, binaryClient.getAndTouch(KEY1, TTL).toCompletableFuture().get());
  }

  @Test
  public void testTouchAndGetNotFound() throws Throwable {
    assumeBinary();

    assertNull(binaryClient.getAndTouch(KEY1, TTL).toCompletableFuture().get());
  }

  @Test
  public void testCasTouchAndGetFound() throws Throwable {
    assumeBinary();

    client.set(KEY1, VALUE1, TTL).toCompletableFuture().get();
    long cas = client.casGet(KEY1).toCompletableFuture().get().getCas();
    GetResult<String> result = binaryClient.casGetAndTouch(KEY1, TTL).toCompletableFuture().get();
    assertEquals(cas, result.getCas());
    assertEquals(VALUE1, result.getValue());
  }

  @Test
  public void testCasTouchAndGetNotFound() throws Throwable {
    assumeBinary();

    GetResult<String> result = binaryClient.casGetAndTouch(KEY1, TTL).toCompletableFuture().get();
    assertNull(result);
  }

  @Test
  public void testNoop() throws Throwable {
    assumeBinary();

    binaryClient.noop().toCompletableFuture().get();
  }

  @Test
  public void testAlmostTooLargeValues() throws Exception {
    String value = createValue(1024 * 1024 - 1000);
    assertEquals(MemcacheStatus.OK, client.set(KEY1, value, TTL).toCompletableFuture().get());
    assertEquals(value, client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testTooLargeValues() throws Exception {
    String value = createValue(2 * 1024 * 1024);
    assertEquals(MemcacheStatus.OK, client.set(KEY1, "", TTL).toCompletableFuture().get());
    assertEquals(
        MemcacheStatus.VALUE_TOO_LARGE,
        client.set(KEY1, value, TTL).toCompletableFuture().get());
    assertEquals("", client.get(KEY1).toCompletableFuture().get());
  }

  @Test
  public void testTooLargeValueDoesNotCorruptConnection() throws Exception {
    String largeValue = createValue(2 * 1024 * 1024);
    client.set(KEY1, largeValue, TTL).toCompletableFuture().get();

    String smallValue = createValue(1000);
    for (int i = 0; i < 1000; i++) {
      assertEquals(
          MemcacheStatus.OK,
          client.set(KEY1, smallValue, TTL).toCompletableFuture().get());
      assertEquals(smallValue, client.get(KEY1).toCompletableFuture().get());
    }
  }

  private String createValue(int size) {
    StringBuilder sb = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      sb.append('.');
    }
    return sb.toString();
  }

  private void assumeAscii() {
    assumeTrue(isAscii());
  }

  private void assumeBinary() {
    assumeTrue(isBinary());
  }

  static void assertGetKeyNotFound(CompletionStage<String> future) throws Throwable {
    checkKeyNotFound(future);
  }

  static void checkKeyNotFound(final CompletionStage<?> future) throws Throwable {
    checkStatus(future, MemcacheStatus.KEY_NOT_FOUND);
  }

  static void checkStatus(final CompletionStage<?> future, final MemcacheStatus... expected)
      throws Throwable {
    final Set<MemcacheStatus> expectedSet = Sets.newHashSet(expected);
    try {
      Object v = future.toCompletableFuture().get();
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

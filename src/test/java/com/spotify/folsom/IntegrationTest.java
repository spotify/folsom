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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class IntegrationTest {

  private static final HostAndPort SERVER_ADDRESS = HostAndPort.fromParts("127.0.0.1", 11211);
  private static EmbeddedServer asciiEmbeddedServer;
  private static EmbeddedServer binaryEmbeddedServer;

  private static boolean memcachedRunning() {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(SERVER_ADDRESS.getHostText(), SERVER_ADDRESS.getPort()));
      return true;
    } catch (ConnectException e) {
      System.err.println("memcached not running, disabling test");
      return false;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Parameterized.Parameters(name="{0}-{1}")
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

  public static void awaitConnected(final MemcacheClient<?> client) {
    while (client != null && client.numActiveConnections() < client.numTotalConnections()) {
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
    }
  }

  @Before
  public void setUp() throws Exception {
    boolean ascii;
    if (protocol.equals("ascii")) {
      ascii = true;
    } else if (protocol.equals("binary")) {
      ascii = false;
    } else {
      throw new IllegalArgumentException(protocol);
    }
    int embeddedPort = ascii ? asciiEmbeddedServer.getPort() : binaryEmbeddedServer.getPort();
    int port = isEmbedded() ? embeddedPort : 11211;

    MemcacheClientBuilder<String> builder = MemcacheClientBuilder.newStringClient()
            .withAddress(HostAndPort.fromParts("127.0.0.1", port))
            .withConnections(1)
            .withMaxOutstandingRequests(100)
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
    awaitConnected(client);
    System.out.println("Using client: " + client + ", protocol: " + protocol + " and port: " + port);
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
    client.shutdown().get();
  }

  protected static final String KEY1 = "folsomtest:key1";
  protected static final String KEY2 = "folsomtest:key2";
  protected static final String KEY3 = "folsomtest:key3";
  protected static final String VALUE1 = "val1";
  protected static final String VALUE2 = "val2";
  protected static final String NUMERIC_VALUE = "123";

  public static final List<String> ALL_KEYS = ImmutableList.of(KEY1, KEY2, KEY3);

  protected static final int TTL = Integer.MAX_VALUE;

  @Test
  public void testSetGet() throws Exception {
    client.set(KEY1, VALUE1, TTL).get();

    assertEquals(VALUE1, client.get(KEY1).get());
  }

  @Test
  public void testLargeSet() throws Exception {
    String value = Collections.nCopies(10000, "Hello world ").toString();
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
    if (binaryClient != null) {
      binaryClient.set(KEY1, VALUE1, TTL).get();
      final long cas = binaryClient.casGet(KEY1).get().getCas();
      binaryClient.append(KEY1, VALUE2, cas).get();

      assertEquals(VALUE1 + VALUE2, binaryClient.get(KEY1).get());
    }
  }

  @Test
  public void testAppendCasIncorrect() throws Throwable {
    if (isEmbedded()) {
      // CAS is broken on embedded server
      return;
    }
    if (binaryClient != null) {
      binaryClient.set(KEY1, VALUE1, TTL).get();
      final long cas = binaryClient.casGet(KEY1).get().getCas();
      checkStatus(binaryClient.append(KEY1, VALUE2, cas + 666), MemcacheStatus.KEY_EXISTS);

    }
  }

  @Test
  public void testPrependCasIncorrect() throws Throwable {
    if (isEmbedded()) {
      // CAS is broken on embedded server
      return;
    }
    if (binaryClient != null) {
      binaryClient.set(KEY1, VALUE1, TTL).get();
      final long cas = binaryClient.casGet(KEY1).get().getCas();
      checkStatus(binaryClient.prepend(KEY1, VALUE2, cas + 666), MemcacheStatus.KEY_EXISTS);
    }
  }

  @Test
  public void testPrependCas() throws Exception {
    if (binaryClient != null) {
      binaryClient.set(KEY1, VALUE1, TTL).get();
      final long cas = binaryClient.casGet(KEY1).get().getCas();
      binaryClient.prepend(KEY1, VALUE2, cas).get();

      assertEquals(VALUE2 + VALUE1, binaryClient.get(KEY1).get());
    }
  }

  @Test
  public void testPrepend() throws Exception {
    client.set(KEY1, VALUE1, TTL).get();
    client.prepend(KEY1, VALUE2).get();

    assertEquals(VALUE2 + VALUE1, client.get(KEY1).get());
  }

  @Test
  public void testSetDeleteGet() throws Throwable {
    if (isEmbedded() && isBinary()) {
      // returns "" instead of NOT_FOUND on embedded server
      return;
    }
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
    if (isEmbedded() && isBinary()) {
      // returns "" instead of NOT_FOUND on embedded server
      return;
    }
    assertGetKeyNotFound(client.get(KEY1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTooLargeMultiget() throws Exception {
    List<String> keys = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      keys.add("key-" + i);
    }
    client.get(keys);
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
    if (isEmbedded() && isBinary()) {
      // returns "" instead of NOT_FOUND on embedded server
      return;
    }
    client.set(KEY1, VALUE1, TTL).get();
    client.set(KEY2, VALUE2, TTL).get();

    assertEquals(asList(VALUE1, null, VALUE2), client.get(Arrays.asList(KEY1, KEY3, KEY2)).get());
  }

  @Test
  public void testDeleteKeyNotExist() throws Throwable {
    checkKeyNotFound(client.delete(KEY1));
  }

  @Test
  public void testFlushAll() throws Exception {
    client.set(KEY1, VALUE1, TTL).get();
    client.flushAll(2);
    assertEquals(VALUE1, client.get(KEY1).get());
    Thread.sleep(2000);
    assertEquals(null, client.get(KEY1).get());

    client.set(KEY1, VALUE1, TTL).get();
    client.flushAll(0);
    assertEquals(null, client.get(KEY1).get());
  }

  @Test
  public void testIncr() throws Throwable {
    if (isEmbedded()) {
      // Incr gives NPE on embedded server
      return;
    }
    if (binaryClient != null) {
      assertEquals(new Long(3), binaryClient.incr(KEY1, 2, 3, TTL).get());
      assertEquals(new Long(5), binaryClient.incr(KEY1, 2, 7, TTL).get());
    }
  }

  private boolean isEmbedded() {
    return type.equals("embedded");
  }

  private boolean isBinary() {
    return protocol.equals("binary");
  }

  @Test
  public void testDecr() throws Throwable {
    if (isEmbedded()) {
      // Decr gives NPE on embedded server
      return;
    }
    if (binaryClient != null) {
      assertEquals(new Long(3), binaryClient.decr(KEY1, 2, 3, TTL).get());
      assertEquals(new Long(1), binaryClient.decr(KEY1, 2, 5, TTL).get());
    }
  }

  @Test
  public void testDecrBelowZero() throws Throwable {
    if (binaryClient != null && !isEmbedded()) { // Decr gives NPE on embedded server
      assertEquals(new Long(3), binaryClient.decr(KEY1, 2, 3, TTL).get());
      // should not go below 0
      assertEquals(new Long(0), binaryClient.decr(KEY1, 4, 5, TTL).get());
    }
  }

  @Test
  public void testIncrDefaults() throws Throwable {
    if (isEmbedded()) {
      // embedded crashes on this
      return;
    }
    // Ascii client behaves differently for this use case
    if (binaryClient != null) {
      assertEquals(new Long(0), binaryClient.incr(KEY1, 2, 0,0).get());
      // memcached will intermittently cause this test to fail due to the value not being set
      // correctly when incr with initial=0 is used, this works around that
      binaryClient.set(KEY1, "0", TTL).get();
      assertEquals(new Long(2), binaryClient.incr(KEY1, 2, 0, 0).get());
    }
  }

  @Test
  public void testIncrDefaultsAscii() throws Throwable {
    // Ascii client behaves differently for this use case
    if (asciiClient != null) {
      assertEquals(null, asciiClient.incr(KEY1, 2).get());
      // memcached will intermittently cause this test to fail due to the value not being set
      // correctly when incr with initial=0 is used, this works around that
      asciiClient.set(KEY1, "0", TTL).get();
      assertEquals(new Long(2), asciiClient.incr(KEY1, 2).get());
    }
  }

  @Test
  public void testIncrAscii() throws Throwable {
    if (asciiClient != null) {
      asciiClient.set(KEY1, NUMERIC_VALUE, TTL).get();
      assertEquals(new Long(125), asciiClient.incr(KEY1, 2).get());
    }
  }

  @Test
  public void testDecrAscii() throws Throwable {
    if (asciiClient != null) {
      asciiClient.set(KEY1, NUMERIC_VALUE, TTL).get();
      assertEquals(new Long(121), asciiClient.decr(KEY1, 2).get());
    }
  }


  @Test
  public void testCas() throws Throwable {
    if (isEmbedded() && isBinary()) {
      // CAS is broken on embedded server
      return;
    }
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
    if (isEmbedded() && isBinary()) {
      // CAS is broken on embedded server
      return;
    }
    client.set(KEY1, VALUE1, TTL).get();

    checkStatus(client.set(KEY1, VALUE2, TTL, 666), MemcacheStatus.KEY_EXISTS);
  }

  @Test
  public void testTouchNotFound() throws Throwable {
    if (isEmbedded()) {
      // touch is broken on embedded server
      return;
    }
    MemcacheStatus result = client.touch(KEY1, TTL).get();
    assertEquals(MemcacheStatus.KEY_NOT_FOUND, result);
  }

  @Test
  public void testTouchFound() throws Throwable {
    if (isEmbedded()) {
      // touch is broken on embedded server
      return;
    }
    client.set(KEY1, VALUE1, TTL).get();
    MemcacheStatus result = client.touch(KEY1, TTL).get();
    assertEquals(MemcacheStatus.OK, result);
  }

  @Test
  public void testNoop() throws Throwable {
    if (isEmbedded()) {
      // CAS is broken on embedded server
      return;
    }
    if (binaryClient != null) {
      binaryClient.noop().get();
    }
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

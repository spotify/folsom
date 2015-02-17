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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.protocol.MemcachedCommandHandler;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class MemcacheClientStressTest {

  private static final String KEY = "mykey";
  private static final byte[] VALUE = "myvalue".getBytes();

  public static final int N = 1000;
  private final EmbeddedServer daemon = new EmbeddedServer(false);
  private ExecutorService workerExecutor;
  private MemcacheClient<byte[]> client;

  @Before
  public void setUp() throws Exception {
    final LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    lc.getLogger(MemcachedCommandHandler.class).setLevel(Level.ERROR);
    workerExecutor = Executors.newFixedThreadPool(100);

    client = MemcacheClientBuilder.newByteArrayClient()
        .withAddress(HostAndPort.fromParts("127.0.0.1", daemon.getPort()))
        .connectAscii();
    IntegrationTest.awaitConnected(client);
  }

  public static void main(final String[] args) throws Exception {
    final MemcacheClientStressTest test = new MemcacheClientStressTest();
    test.setUp();
    try {
      test.stressTest();
    } finally {
      test.tearDown();
    }
  }

  @Test
  @Ignore
  public void stressTest() throws Exception {
    client.set(KEY, VALUE, 100000).get();

    while (true) {
      final List<ListenableFuture<byte[]>> futures = Lists.newArrayList();

      final AtomicInteger successes = new AtomicInteger();
      final ConcurrentMap<String, AtomicInteger> failures = Maps.newConcurrentMap();
      for (int i = 0; i < N; i++) {
        addRequest(client, futures, successes, failures);
      }

      client.shutdown();
      client = MemcacheClientBuilder.newByteArrayClient()
          .withAddress(HostAndPort.fromParts("127.0.0.1", daemon.getPort()))
          .connectBinary();
      Futures.successfulAsList(futures).get();

      System.out.println("success: " + successes.get());
      for (final Map.Entry<String, AtomicInteger> entry : failures.entrySet()) {
        System.out.println("failure: " + entry.getKey() + " = " + entry.getValue().get());
      }

      int totalFails = 0;
      for (final AtomicInteger integer : failures.values()) {
        totalFails += integer.get();
      }
      assertEquals(N, successes.get() + totalFails);
    }
  }

  private void addRequest(final MemcacheClient<byte[]> client, final List<ListenableFuture<byte[]>> futures, final AtomicInteger successes, final ConcurrentMap<String, AtomicInteger> failures) {
    final ListenableFuture<byte[]> future = client.get(KEY);
    Futures.addCallback(future, new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(final byte[] result) {
        successes.incrementAndGet();
      }

      @Override
      public void onFailure(final Throwable t) {
        final AtomicInteger newCounter = new AtomicInteger();
        String message = t.getMessage();
        if (message == null) {
          message = "";
        }
        final AtomicInteger old = failures.putIfAbsent(message, newCounter);
        if (old == null) {
          newCounter.incrementAndGet();
        } else {
          old.incrementAndGet();
        }
      }
    });
    futures.add(future);
  }

  @After
  public void tearDown() throws ExecutionException, InterruptedException {
    client.shutdown();
    daemon.stop();
    workerExecutor.shutdown();
  }


}

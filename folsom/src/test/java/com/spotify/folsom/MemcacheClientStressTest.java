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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.spotify.futures.CompletableFutures;
import java.util.concurrent.CompletionStage;

import com.spotify.folsom.client.Utils;
import com.thimbleware.jmemcached.protocol.MemcachedCommandHandler;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

import static org.junit.Assert.assertEquals;

public class MemcacheClientStressTest {

  private static final String KEY = "mykey";
  private static final byte[] VALUE = "myvalue".getBytes();

  public static final int N = 1000;
  private MemcachedServer server;
  private ExecutorService workerExecutor;
  private MemcacheClient<byte[]> client;

  @Before
  public void setUp() throws Exception {
    server = new MemcachedServer();
    assertEquals(0, Utils.getGlobalConnectionCount());

    final LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    lc.getLogger(MemcachedCommandHandler.class).setLevel(Level.ERROR);
    workerExecutor = Executors.newFixedThreadPool(100);


    client = MemcacheClientBuilder.newByteArrayClient()
        .withAddress(server.getHost(), server.getPort())
        .connectAscii();
    client.awaitConnected(10, TimeUnit.SECONDS);
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
    client.set(KEY, VALUE, 100000).toCompletableFuture().get();

    while (true) {
      final List<CompletionStage<byte[]>> futures = Lists.newArrayList();

      final AtomicInteger successes = new AtomicInteger();
      final ConcurrentMap<String, AtomicInteger> failures = Maps.newConcurrentMap();
      for (int i = 0; i < N; i++) {
        addRequest(client, futures, successes, failures);
      }

      client.shutdown();
      client = MemcacheClientBuilder.newByteArrayClient()
          .withAddress(server.getHost(), server.getPort())
          .connectBinary();
      CompletableFutures.allAsList(futures).get();

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

  private void addRequest(final MemcacheClient<byte[]> client,
                          final List<CompletionStage<byte[]>> futures,
                          final AtomicInteger successes,
                          final ConcurrentMap<String, AtomicInteger> failures) {
    final CompletionStage<byte[]> future = client.get(KEY);
    future.whenComplete((result, t) -> {
      if (t == null) {
        successes.incrementAndGet();
      } else {
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
  public void tearDown() throws InterruptedException, TimeoutException {
    client.shutdown();
    server.stop();
    workerExecutor.shutdown();
    client.awaitDisconnected(10, TimeUnit.SECONDS);
    assertEquals(0, Utils.getGlobalConnectionCount());
  }


}

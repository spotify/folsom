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
/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.folsom;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.DAYS;

public class SimpleMemcacheClientBenchmark {

  public static final int MULTIGET_SIZE = 50;

  public static final int CONCURRENCY = 200;

  public static final boolean TEST_SPYMEMCACHED = false;
  public static final boolean ASCII_PROTOCOL = false;

  public static final int NUM_CLIENT_CONNECTIONS = 3;

  public static void main(final String[] args)
      throws ExecutionException, InterruptedException, IOException, TimeoutException {
    // Set up client

    BinaryMemcacheClient<String> client;
    MemcachedClient spyClient;
    if (TEST_SPYMEMCACHED) {
      final DefaultConnectionFactory defaultConnectionFactory = new DefaultConnectionFactory() {
        @Override
        public OperationFactory getOperationFactory() {
          if (ASCII_PROTOCOL) {
            return new AsciiOperationFactory();
          } else {
            return new BinaryOperationFactory();
          }
        }
      };
      spyClient = new MemcachedClient(defaultConnectionFactory,
          Collections.nCopies(NUM_CLIENT_CONNECTIONS, new InetSocketAddress("localhost", 11211)));
    } else {
      client = MemcacheClientBuilder.newStringClient()
              .withMaxOutstandingRequests(100000)
              .withAddress("127.0.0.1", 11211)
              .withConnections(NUM_CLIENT_CONNECTIONS)
              .withRetry(false)
              .connectBinary();
      client.awaitConnected(10, TimeUnit.SECONDS);
      System.out.println(client);
    }
    // Set up test data

    final List<List<String>> keys = Lists.newArrayList();
    final List<List<String>> values = Lists.newArrayList();

    final List<CompletionStage<MemcacheStatus>> futures = Lists.newArrayList();
    for (int i = 0; i < CONCURRENCY; i++) {
      final List<String> keys2 = Lists.newArrayList();
      final List<String> values2 = Lists.newArrayList();
      for (int j = 0; j < MULTIGET_SIZE; j++) {
        final String key = String.format("key-%020d-%010d", i, j);
        final String value = String.format("value-%0200d-%0200d", i, j);
        keys2.add(key);
        values2.add(value);
        if (TEST_SPYMEMCACHED) {
          final CompletableFuture<MemcacheStatus> settable = new CompletableFuture<>();
          spyClient.set(key, Integer.MAX_VALUE, value)
              .addListener(future -> {
                if (future.getStatus().getStatusCode() == StatusCode.SUCCESS) {
                  settable.complete(null);
                } else {
                  settable.completeExceptionally(
                      new RuntimeException(future.getStatus().getMessage()));
                }
              });
          futures.add(settable);
        } else {
          futures.add(client.set(key, value, Integer.MAX_VALUE));
        }
      }
      keys.add(keys2);
      values.add(values2);
    }
    for (final CompletionStage<MemcacheStatus> future : futures) {
      future.toCompletableFuture().get();
    }

    // Run benchmark
    final ScheduledExecutorService backoffExecutor = Executors.newSingleThreadScheduledExecutor();
    final ProgressMeter meter = new ProgressMeter("req");

    for (int i = 0; i < CONCURRENCY; i++) {
      if (TEST_SPYMEMCACHED) {
        sendSpyMemcached(spyClient,
            keys.get(i),
            toMap(keys.get(i), values.get(i)),
            meter,
            backoffExecutor);
      } else {
        sendFolsom(client, keys.get(i), values.get(i), meter, backoffExecutor);
      }
    }

    while (true) {
      Uninterruptibles.sleepUninterruptibly(1, DAYS);
    }
  }

  private static Map<String, String> toMap(final List<String> keys, final List<String> values) {
    final HashMap<String, String> map = Maps.newHashMap();
    for (int i = 0; i < keys.size(); i++) {
      map.put(keys.get(i), values.get(i));
    }
    return map;
  }

  private static void sendFolsom(final BinaryMemcacheClient<String> client,
                                 final List<String> keys, final List<String> expected,
                                 final ProgressMeter meter,
                                 final ScheduledExecutorService backoffExecutor) {
    final CompletionStage<List<String>> future = client.get(keys);
    final long start = System.nanoTime();
    future.whenComplete((response, throwable) -> {
      if (throwable == null) {
        final long end = System.nanoTime();
        final long latency = end - start;
        meter.inc(keys.size(), latency);
        if (!expected.equals(response)) {
          throw new AssertionError("expected: " + expected + ", got: " + response);
        }
        sendFolsom(client, keys, expected, meter, backoffExecutor);
      } else {
        System.err.println(throwable.getMessage());
      }
    });
  }

  private static void sendSpyMemcached(final MemcachedClient client,
                                       final List<String> keys,
                                       final Map<String, String> expectedMap,
                                       final ProgressMeter meter,
                                       final ScheduledExecutorService backoffExecutor) {
    final long start = System.nanoTime();
    final BulkFuture<Map<String, Object>> future = client.asyncGetBulk(keys);
    future.addListener(getFuture -> {
      final OperationStatus status = getFuture.getStatus();
      if (status.isSuccess()) {
        final Map<String, String> response = (Map<String, String>) getFuture.get();
        final long end = System.nanoTime();
        final long latency = end - start;
        meter.inc(keys.size(), latency);
        if (!expectedMap.equals(response)) {
          throw new AssertionError("expected: " + expectedMap + ", got: " + response);
        }
        sendSpyMemcached(client, keys, expectedMap, meter, backoffExecutor);
      } else {
        System.err.println("failure!");
      }
    });
  }
}

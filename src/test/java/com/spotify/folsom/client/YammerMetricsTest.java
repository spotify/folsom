/*
 * Copyright (c) 2015 Spotify AB
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
package com.spotify.folsom.client;

import com.google.common.base.Charsets;
import com.spotify.folsom.AsciiMemcacheClient;
import com.spotify.folsom.FakeRawMemcacheClient;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.transcoder.StringTranscoder;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class YammerMetricsTest {

  private YammerMetrics metrics;
  private AsciiMemcacheClient<String> client;
  private FakeRawMemcacheClient fakeRawMemcacheClient;

  @Before
  public void setUp() throws Exception {
    metrics = new YammerMetrics(new MetricsRegistry());
    fakeRawMemcacheClient = new FakeRawMemcacheClient(metrics);
    client = new DefaultAsciiMemcacheClient<>(
        fakeRawMemcacheClient,
        metrics,
        StringTranscoder.UTF8_INSTANCE,
        Charsets.UTF_8, MemcacheEncoder.MAX_KEY_LEN);
    client.awaitConnected(10, TimeUnit.SECONDS);
  }

  @Test
  public void testGetMiss() throws Exception {
    assertEquals(0, metrics.getGets().count());
    assertEquals(0, metrics.getGetHits().count());
    assertEquals(0, metrics.getGetMisses().count());
    assertEquals(0, metrics.getGetFailures().count());
    assertEquals(0, metrics.getGetSuccesses().count());

    assertNull(client.get("key-miss").toCompletableFuture().get());

    awaitCount(1, metrics.getGets());
    assertEquals(0, metrics.getGetHits().count());
    assertEquals(1, metrics.getGetMisses().count());
    assertEquals(0, metrics.getGetFailures().count());
    assertEquals(1, metrics.getGetSuccesses().count());
  }

  @Test
  public void testGetHit() throws Exception {
    assertEquals(0, metrics.getGets().count());
    assertEquals(0, metrics.getGetHits().count());
    assertEquals(0, metrics.getGetMisses().count());
    assertEquals(0, metrics.getGetFailures().count());
    assertEquals(0, metrics.getGetSuccesses().count());

    assertEquals(
        MemcacheStatus.OK,
        client.set("key", "value", 0).toCompletableFuture().get());
    assertEquals("value", client.get("key").toCompletableFuture().get());

    awaitCount(1, metrics.getGets());
    assertEquals(1, metrics.getGetHits().count());
    assertEquals(0, metrics.getGetMisses().count());
    assertEquals(0, metrics.getGetFailures().count());
    assertEquals(1, metrics.getGetSuccesses().count());
  }

  @Test
  public void testMultiget() throws Exception {
    assertEquals(0, metrics.getMultigets().count());
    assertEquals(0, metrics.getGetHits().count());
    assertEquals(0, metrics.getGetMisses().count());
    assertEquals(0, metrics.getMultigetFailures().count());
    assertEquals(0, metrics.getMultigetSuccesses().count());

    assertEquals(MemcacheStatus.OK, client.set("key", "value", 0).toCompletableFuture().get());
    assertEquals(
        Arrays.asList("value", null),
        client.get(Arrays.asList("key", "key-miss")).toCompletableFuture().get());

    awaitCount(1, metrics.getMultigets());
    assertEquals(1, metrics.getGetHits().count());
    assertEquals(1, metrics.getGetMisses().count());
    assertEquals(0, metrics.getMultigetFailures().count());
    assertEquals(1, metrics.getMultigetSuccesses().count());
  }

  @Test
  public void testSet() throws Exception {
    assertEquals(0, metrics.getSets().count());
    assertEquals(0, metrics.getSetFailures().count());
    assertEquals(0, metrics.getSetSuccesses().count());

    assertEquals(MemcacheStatus.OK, client.set("key", "value", 0).toCompletableFuture().get());

    awaitCount(1, metrics.getSets());
    assertEquals(0, metrics.getSetFailures().count());
    assertEquals(1, metrics.getSetSuccesses().count());
  }

  @Test
  public void testIncrDecr() throws Exception {
    assertEquals(MemcacheStatus.OK, client.set("key", "0", 0).toCompletableFuture().get());

    assertEquals(0, metrics.getIncrDecrs().count());
    assertEquals(0, metrics.getIncrDecrFailures().count());
    assertEquals(0, metrics.getIncrDecrSuccesses().count());

    assertEquals(Long.valueOf(1L), client.incr("key", 1).toCompletableFuture().get());

    awaitCount(1, metrics.getIncrDecrs());
    assertEquals(0, metrics.getIncrDecrFailures().count());
    assertEquals(1, metrics.getIncrDecrSuccesses().count());
  }

  @Test
  public void testTouch() throws Exception {
    assertEquals(MemcacheStatus.OK, client.set("key", "0", 0).toCompletableFuture().get());

    assertEquals(0, metrics.getTouches().count());
    assertEquals(0, metrics.getTouchFailures().count());
    assertEquals(0, metrics.getTouchSuccesses().count());

    assertEquals(MemcacheStatus.OK, client.touch("key", 1).toCompletableFuture().get());

    awaitCount(1, metrics.getTouches());
    assertEquals(0, metrics.getTouchFailures().count());
    assertEquals(1, metrics.getTouchSuccesses().count());
  }

  @Test
  public void testDelete() throws Exception {
    assertEquals(0, metrics.getDeletes().count());
    assertEquals(0, metrics.getDeleteFailures().count());
    assertEquals(0, metrics.getDeleteSuccesses().count());

    assertEquals(MemcacheStatus.OK, client.delete("key").toCompletableFuture().get());

    awaitCount(1, metrics.getDeletes());
    assertEquals(0, metrics.getDeleteFailures().count());
    assertEquals(1, metrics.getDeleteSuccesses().count());
  }

  /** Test wiring up of OutstandingRequestGauge to the Yammer-metrics gauge. */
  @Test
  public void testOutstandingRequests() throws Exception {
    // baseline
    assertEquals(0, metrics.getOutstandingRequestsGauge().value().intValue());

    fakeRawMemcacheClient.setOutstandingRequests(5);
    assertEquals(5, metrics.getOutstandingRequestsGauge().value().intValue());

    fakeRawMemcacheClient.setOutstandingRequests(0);
    assertEquals(0, metrics.getOutstandingRequestsGauge().value().intValue());
  }

  private void awaitCount(int expectedValue, Metered timer) throws InterruptedException {
    final int timeout = 10;
    final long t1 = System.currentTimeMillis();
    while (expectedValue != timer.count()) {
      if (System.currentTimeMillis() - t1 > timeout) {
        assertEquals(expectedValue, timer.count());
        return;
      }
      Thread.sleep(0, 100);
    }
  }
}

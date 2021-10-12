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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.spotify.folsom.AsciiMemcacheClient;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.client.test.FakeRawMemcacheClient;
import com.spotify.folsom.transcoder.StringTranscoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

public class MicrometerMetricsTest {

  private MicrometerMetrics metrics;
  private SimpleMeterRegistry meterRegistry;
  private AsciiMemcacheClient<String> client;
  private FakeRawMemcacheClient fakeRawMemcacheClient;

  @Before
  public void setUp() throws Exception {
    setupComponents();
  }

  private void setupComponents(String... tags) throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    if(tags.length == 0) {
      metrics = new MicrometerMetrics(meterRegistry);
    } else {
      metrics = new MicrometerMetrics(meterRegistry, tags);
    }
    fakeRawMemcacheClient = new FakeRawMemcacheClient(metrics);
    client =
            new DefaultAsciiMemcacheClient<>(
                    fakeRawMemcacheClient,
                    metrics,
                    NoopTracer.INSTANCE,
                    StringTranscoder.UTF8_INSTANCE,
                    StandardCharsets.UTF_8,
                    MemcacheEncoder.MAX_KEY_LEN);
    client.awaitConnected(10, TimeUnit.SECONDS);
  }

  @Test
  public void testExtraTags() throws Exception {
    setupComponents("client", "a");

    assertNull(client.get("key-miss").toCompletableFuture().get());
    Timer getMisses = meterRegistry.find("memcache.requests")
            .tag("operation", "get")
            .tag("result", "misses")
            .tag("client", "a").timer();
    assert getMisses != null;
    awaitCount(1, getMisses);
  }


  @Test
  public void testGetMiss() throws Exception {

    assertEquals(0, getGetHits().count());
    assertEquals(0, getGetMisses().count(), 0);
    assertEquals(0, getGetFailures().count(), 0);

    assertNull(client.get("key-miss").toCompletableFuture().get());

    awaitCount(1, getGetMisses());
    assertEquals(0, getGetHits().count(), 0);
    assertEquals(1, getGetMisses().count(), 0);
    assertEquals(0, getGetFailures().count(), 0);
  }

  @Test
  public void testGetHit() throws Exception {
    assertEquals(0, getGetHits().count(), 0);
    assertEquals(0, getGetMisses().count(), 0);
    assertEquals(0, getGetFailures().count(), 0);

    assertEquals(MemcacheStatus.OK, client.set("key", "value", 0).toCompletableFuture().get());
    assertEquals("value", client.get("key").toCompletableFuture().get());

    assertEquals(1, getGetHits().count(), 0);
    assertEquals(0, getGetMisses().count(), 0);
    assertEquals(0, getGetFailures().count(), 0);
  }

  @Test
  public void testMultiget() throws Exception {
    assertEquals(0, getMultigetCalls().count());
    assertEquals(0, getGetHits().count(), 0);
    assertEquals(0, getGetMisses().count(), 0);
    assertEquals(0, getMultigetFailures().count(), 0);

    assertEquals(MemcacheStatus.OK, client.set("key", "value", 0).toCompletableFuture().get());
    assertEquals(
        Arrays.asList("value", null),
        client.get(Arrays.asList("key", "key-miss")).toCompletableFuture().get());

    awaitCount(1, getMultigetCalls());
    assertEquals(1, getMultigetHits().count(), 0);
    assertEquals(1, getMultigetMisses().count(), 0);
    assertEquals(0, getMultigetFailures().count(), 0);
  }

  @Test
  public void testSet() throws Exception {
    assertEquals(0, getSetFailures().count(), 0);
    assertEquals(0, getSetSuccesses().count(), 0);

    assertEquals(MemcacheStatus.OK, client.set("key", "value", 0).toCompletableFuture().get());

    assertEquals(0, getSetFailures().count(), 0);
    assertEquals(1, getSetSuccesses().count(), 0);
  }

  @Test
  public void testIncrDecr() throws Exception {
    assertEquals(MemcacheStatus.OK, client.set("key", "0", 0).toCompletableFuture().get());

    assertEquals(0, getIncrDecrFailures().count(), 0);
    assertEquals(0, getIncrDecrSuccesses().count(), 0);

    assertEquals(Long.valueOf(1L), client.incr("key", 1).toCompletableFuture().get());

    assertEquals(0, getIncrDecrFailures().count(), 0);
    assertEquals(1, getIncrDecrSuccesses().count(), 0);
  }

  @Test
  public void testTouch() throws Exception {
    assertEquals(MemcacheStatus.OK, client.set("key", "0", 0).toCompletableFuture().get());

    assertEquals(0, getTouchFailures().count(), 0);
    assertEquals(0, getTouchSuccesses().count(), 0);

    assertEquals(MemcacheStatus.OK, client.touch("key", 1).toCompletableFuture().get());

    assertEquals(0, getTouchFailures().count(), 0);
    assertEquals(1, getTouchSuccesses().count(), 0);
  }

  @Test
  public void testDelete() throws Exception {
    assertEquals(0, getDeleteFailures().count(), 0);
    assertEquals(0, getDeleteSuccesses().count(), 0);

    assertEquals(MemcacheStatus.OK, client.delete("key").toCompletableFuture().get());

    assertEquals(0, getDeleteFailures().count(), 0);
    assertEquals(1, getDeleteSuccesses().count(), 0);
  }

  /** Test wiring up of OutstandingRequestGauge to the Yammer-metrics gauge. */
  @Test
  public void testOutstandingRequests() {
    // baseline
    assertEquals(0L, metrics.getOutstandingRequests());

    fakeRawMemcacheClient.setOutstandingRequests(5);
    assertEquals(5L, metrics.getOutstandingRequests());

    fakeRawMemcacheClient.setOutstandingRequests(0);
    assertEquals(0L, metrics.getOutstandingRequests());
  }

  private void awaitCount(int expectedValue, Timer timer) throws InterruptedException {
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

//        registry.gauge("memcache.outstandingRequests", tags, this, MicrometerMetrics::getOutstandingRequests);
//        registry.gauge("memcache.global.connections", tags, this, o -> Utils.getGlobalConnectionCount());


  private Timer getGetHits() {
    return meterRegistry.find("memcache.requests").tag("operation", "get").tag("result", "hits").timer();
  }

  private Timer getGetMisses() {
    return meterRegistry.find("memcache.requests").tag("operation", "get").tag("result", "misses").timer();
  }

  private Timer getGetFailures() {
    return meterRegistry.find("memcache.requests").tag("operation", "get").tag("result", "failures").timer();
  }

  private Timer getMultigetCalls() {
    return meterRegistry.find("memcache.requests").tag("operation", "multiget").tag("result", "hitsOrMisses").timer();
  }

  private Counter getMultigetHits() {
    return meterRegistry.find("memcache.requests").tag("operation", "multiget").tag("result", "hits").counter();
  }

  private Counter getMultigetMisses() {
    return meterRegistry.find("memcache.requests").tag("operation", "multiget").tag("result", "misses").counter();
  }

  private Timer getMultigetFailures() {
    return meterRegistry.find("memcache.requests").tag("operation", "multiget").tag("result", "failures").timer();
  }

  private Timer getSetSuccesses() {
    return meterRegistry.find("memcache.requests").tag("operation", "set").tag("result", "successes").timer();
  }

  private Timer getSetFailures() {
    return meterRegistry.find("memcache.requests").tag("operation", "set").tag("result", "failures").timer();
  }

  private Timer getDeleteSuccesses() {
    return meterRegistry.find("memcache.requests").tag("operation", "delete").tag("result", "successes").timer();
  }

  private Timer getDeleteFailures() {
    return meterRegistry.find("memcache.requests").tag("operation", "delete").tag("result", "failures").timer();
  }

  private Timer getIncrDecrSuccesses() {
    return meterRegistry.find("memcache.requests").tag("operation", "incrdecr").tag("result", "successes").timer();
  }

  private Timer getIncrDecrFailures() {
    return meterRegistry.find("memcache.requests").tag("operation", "incrdecr").tag("result", "failures").timer();
  }

  private Timer getTouchSuccesses() {
    return meterRegistry.find("memcache.requests").tag("operation", "touch").tag("result", "successes").timer();
  }

  private Timer getTouchFailures() {
    return meterRegistry.find("memcache.requests").tag("operation", "touch").tag("result", "failures").timer();
  }

}

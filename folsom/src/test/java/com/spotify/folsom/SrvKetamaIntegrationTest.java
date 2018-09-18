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

package com.spotify.folsom;

import com.spotify.futures.CompletableFutures;
import com.google.common.collect.Lists;
import java.util.concurrent.CompletionStage;
import com.spotify.dns.LookupResult;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Utils;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SrvKetamaIntegrationTest {

  private static KetamaIntegrationTest.Servers servers;

  private MemcacheClient<String> client;

  @BeforeClass
  public static void setUpClass() throws Exception {
    servers = new KetamaIntegrationTest.Servers(3);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    servers.stop();
  }

  @Before
  public void setUp() throws Exception {
    assertEquals(0, Utils.getGlobalConnectionCount());

    MemcacheClientBuilder<String> builder = MemcacheClientBuilder.newStringClient()
            .withSRVRecord("memcached.srv")
            .withSrvResolver(s -> toResult(servers.getServers()))
            .withSRVShutdownDelay(1000)
            .withMaxOutstandingRequests(10000)
            .withMetrics(NoopMetrics.INSTANCE)
            .withRetry(false)
            .withRequestTimeoutMillis(10 * 1000);
    client = builder.connectAscii();

    KetamaIntegrationTest.allClientsConnected(client);
    client.flushAll(0).toCompletableFuture().get();
  }

  public static List<LookupResult> toResult(List<MemcachedServer> servers) {
    return servers.stream()
        .map(server -> LookupResult.create(server.getHost(), server.getPort(), 100, 100, 100))
        .collect(Collectors.toList());
  }

  @After
  public void tearDown() throws Exception {
    client.shutdown();
    client.awaitDisconnected(10, TimeUnit.SECONDS);

    assertEquals(0, Utils.getGlobalConnectionCount());
  }

  @Test
  public void testSetGet() throws Exception {
    List<CompletionStage<?>> futures = Lists.newArrayList();
    final int numKeys = 1000;
    for (int i = 0; i < numKeys; i++) {
      CompletionStage<MemcacheStatus> future = client.set("key-" + i, "value-" + i, 0);
      futures.add(future);

      // Do this to avoid making the embedded memcached sad
      if (i % 10 == 0) {
        future.toCompletableFuture().get();
      }
    }
    CompletableFutures.allAsList(futures).get();
    futures.clear();

    for (int i = 0; i < numKeys; i++) {
      CompletionStage<String> future = client.get("key-" + i);
      futures.add(future);

      // Do this to avoid making the embedded memcached sad
      if (i % 10 == 0) {
        future.toCompletableFuture().get();
      }
    }
    for (int i = 0; i < numKeys; i++) {
      assertEquals("value-" + i, futures.get(i).toCompletableFuture().get());
    }
    futures.clear();

    servers.getInstance(1).stop();

    // TODO: make this prettier
    while (client.numActiveConnections() == client.numTotalConnections()) {
      Thread.sleep(1);
    }

    assertTrue(client.numActiveConnections() == client.numTotalConnections() - 1);

    for (int i = 0; i < numKeys; i++) {
      CompletionStage<String> future = client.get("key-" + i);
      futures.add(future);

      // Do this to avoid making the embedded memcached sad
      if (i % 10 == 0) {
        future.toCompletableFuture().get();
      }

    }
    int misses = 0;
    for (int i = 0; i < numKeys; i++) {
      misses += futures.get(i).toCompletableFuture().get() == null ? 1 : 0;
    }

    // About 1/3 should be misses.
    // Due to random ports in the embedded server, this is actually somewhat non-deterministic.
    // This is why we use a large number of keys.
    double missWithPerfectDistribution = numKeys / servers.getServers().size();
    double diff = Math.abs(misses - missWithPerfectDistribution);
    double relativeDiff = diff / numKeys;
    assertTrue("Misses: " + misses, relativeDiff < 0.2);
  }

}

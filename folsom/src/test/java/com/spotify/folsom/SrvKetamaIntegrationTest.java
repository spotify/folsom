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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.spotify.dns.LookupResult;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Utils;
import com.spotify.futures.CompletableFutures;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SrvKetamaIntegrationTest {

  private KetamaIntegrationTest.Servers servers;

  private MemcacheClient<String> client;
  private int connections;

  @Before
  public void setUp() throws Exception {
    connections = Utils.getGlobalConnectionCount();
    servers = new KetamaIntegrationTest.Servers(3);

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
    servers.flush();
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
    servers.stop();
    assertEquals(connections, Utils.getGlobalConnectionCount());
  }

  @Test
  public void testSetGet() throws Exception {
    List<CompletionStage<?>> futures = Lists.newArrayList();
    final int numKeys = 1000;
    for (int i = 0; i < numKeys; i++) {
      CompletionStage<MemcacheStatus> future = client.set("key-" + i, "value-" + i, 0);
      futures.add(future);
    }
    CompletableFutures.allAsList(futures).get();
    futures.clear();

    for (int i = 0; i < numKeys; i++) {
      CompletionStage<String> future = client.get("key-" + i);
      futures.add(future);
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
    }
    int misses = 0;
    for (int i = 0; i < numKeys; i++) {
      misses += futures.get(i).toCompletableFuture().get() == null ? 1 : 0;
    }

    // About 1/3 should be misses.
    // Due to random ports in the server, this is actually somewhat non-deterministic.
    // This is why we use a large number of keys.
    double missWithPerfectDistribution = numKeys / servers.getServers().size();
    double diff = Math.abs(misses - missWithPerfectDistribution);
    double relativeDiff = diff / numKeys;
    assertTrue("Misses: " + misses, relativeDiff < 0.2);
  }

}

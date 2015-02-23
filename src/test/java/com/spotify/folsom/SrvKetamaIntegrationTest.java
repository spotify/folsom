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

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Utils;
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
    servers = new KetamaIntegrationTest.Servers(3, false);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    servers.stop();
  }

  @Before
  public void setUp() throws Exception {
    MemcacheClientBuilder<String> builder = MemcacheClientBuilder.newStringClient()
            .withSRVRecord("memcached.srv")
            .withSrvResolver(new DnsSrvResolver() {
              @Override
              public List<HostAndPort> resolve(String s) {
                return servers.getAddresses();
              }
            })
            .withSRVRefreshPeriod(1000)
            .withSRVShutdownDelay(1000)
            .withMaxOutstandingRequests(10000)
            .withMetrics(NoopMetrics.INSTANCE)
            .withRetry(false)
            .withReplyExecutor(Utils.SAME_THREAD_EXECUTOR)
            .withRequestTimeoutMillis(10*1000);
    client = builder.connectAscii();

    KetamaIntegrationTest.allClientsConnected(client);
    servers.flush();
  }

  @After
  public void tearDown() throws Exception {
    client.shutdown();
    ConnectFuture.disconnectFuture(client).get();
  }

  @Test
  public void testSetGet() throws Exception {
    List<ListenableFuture<?>> futures = Lists.newArrayList();
    int NUM_KEYS = 1000;
    for (int i = 0; i < NUM_KEYS; i++) {
      futures.add(client.set("key-" + i, "value-" + i, 0));
    }
    Futures.allAsList(futures).get();
    futures.clear();

    for (int i = 0; i < NUM_KEYS; i++) {
      futures.add(client.get("key-" + i));
    }
    for (int i = 0; i < NUM_KEYS; i++) {
      assertEquals("value-" + i, futures.get(i).get());
    }
    futures.clear();

    servers.getInstance(1).stop();

    // TODO: make this prettier
    while (client.numActiveConnections() == client.numTotalConnections()) {
      Thread.sleep(1);
    }

    assertTrue(client.numActiveConnections() == client.numTotalConnections() - 1);

    for (int i = 0; i < NUM_KEYS; i++) {
      futures.add(client.get("key-" + i));
    }
    int misses = 0;
    for (int i = 0; i < NUM_KEYS; i++) {
      misses += futures.get(i).get() == null ? 1 : 0;
    }

    // About 1/3 should be misses.
    // Due to random ports in the embedded server, this is actually somewhat non-deterministic.
    // This is why we use a large number of keys.
    double missWithPerfectDistribution = NUM_KEYS / servers.getAddresses().size();
    double diff = Math.abs(misses - missWithPerfectDistribution);
    double relativeDiff = diff / NUM_KEYS;
    assertTrue("Misses: " + misses, relativeDiff < 0.2);
  }

}

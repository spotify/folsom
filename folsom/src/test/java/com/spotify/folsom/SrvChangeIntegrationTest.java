/*
 * Copyright (c) 2017 Spotify AB
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

import static com.spotify.folsom.SrvKetamaIntegrationTest.toResult;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.LookupResult;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Utils;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.ketama.SrvKetamaClient;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SrvChangeIntegrationTest {

  private static KetamaIntegrationTest.Servers servers;

  private MemcacheClient<String> client;
  private DnsSrvResolver dnsSrvResolver;
  private List<LookupResult> fullResults;
  private List<LookupResult> oneMissing;
  private SrvKetamaClient srvKetamaClient;

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
    assertEquals(0, Utils.getGlobalConnectionCount());

    fullResults = toResult(servers.getPorts());
    oneMissing = ImmutableList.copyOf(
        fullResults.subList(0, fullResults.size() - 1));

    dnsSrvResolver = mock(DnsSrvResolver.class);
    when(dnsSrvResolver.resolve(anyString())).thenReturn(fullResults);

    MemcacheClientBuilder<String> builder = MemcacheClientBuilder.newStringClient()
        .withSRVRecord("memcached.srv")
        .withSrvResolver(dnsSrvResolver)
        .withSRVRefreshPeriod(1)
        .withSRVShutdownDelay(0)
        .withMaxOutstandingRequests(10000)
        .withMetrics(NoopMetrics.INSTANCE)
        .withRetry(false)
        .withRequestTimeoutMillis(10 * 1000);
    client = builder.connectAscii();

    DefaultAsciiMemcacheClient client2 = (DefaultAsciiMemcacheClient) this.client;
    srvKetamaClient = (SrvKetamaClient) client2.getRawMemcacheClient();


    KetamaIntegrationTest.allClientsConnected(client);
    servers.flush();
  }

  @After
  public void tearDown() throws Exception {
    client.shutdown();
    client.awaitDisconnected(10, TimeUnit.SECONDS);

    waitUntilSuccess(1000, () -> assertEquals(0, Utils.getGlobalConnectionCount()));
  }

  @Test
  public void testFlappingSrv() throws Exception {
    for (int i = 0; i < 10; i++) {
      when(dnsSrvResolver.resolve(anyString())).thenReturn(fullResults);
      srvKetamaClient.updateDNS();
      waitUntilSuccess(
          1000,
          () -> assertEquals("Full results (3)", fullResults.size(), client.numActiveConnections())
      );

      when(dnsSrvResolver.resolve(anyString())).thenReturn(oneMissing);
      srvKetamaClient.updateDNS();
      waitUntilSuccess(
          1000,
          () -> assertEquals("One missing (2)", oneMissing.size(), client.numActiveConnections())
      );
    }
  }

  private static void waitUntilSuccess(long timeout, Runnable runnable)
      throws InterruptedException {
    long t1 = System.currentTimeMillis();
    long sleepTime = 1;
    while (System.currentTimeMillis() - t1 < timeout) {
      try {
        runnable.run();
        return;
      } catch (Throwable e) {
        Thread.sleep(sleepTime);
        sleepTime *= 2;
      }
    }
    runnable.run();
  }

}

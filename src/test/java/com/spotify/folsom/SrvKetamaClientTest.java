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

import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.folsom.ketama.SrvKetamaClient;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SrvKetamaClientTest {
  @Test
  public void testSimple() throws Exception {
    HostAndPort hostNameA = HostAndPort.fromHost("a");
    HostAndPort hostNameB = HostAndPort.fromHost("b");
    HostAndPort hostNameC = HostAndPort.fromHost("c");
    HostAndPort hostNameD = HostAndPort.fromHost("d");

    DnsSrvResolver resolver = Mockito.mock(DnsSrvResolver.class);

    // Run shutdown code immediately
    ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
    Mockito.when(executor.schedule(Mockito.any(Runnable.class),
            Mockito.anyLong(), Mockito.any(TimeUnit.class)))
            .thenAnswer(new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
                runnable.run();
                return null;
              }
            });


    final Map<HostAndPort, FakeRawMemcacheClient> knownClients = Maps.newHashMap();

    SrvKetamaClient.Connector connector = new SrvKetamaClient.Connector() {
      @Override
      public RawMemcacheClient connect(HostAndPort input) {
        FakeRawMemcacheClient fakeRawMemcacheClient = new FakeRawMemcacheClient();
        knownClients.put(input, fakeRawMemcacheClient);
        return fakeRawMemcacheClient;
      }
    };

    SrvKetamaClient ketamaClient = new SrvKetamaClient(
            "the-srv-record", resolver, executor, 1000, TimeUnit.MILLISECONDS,
            connector, 1000, TimeUnit.MILLISECONDS);

    assertFalse(ketamaClient.isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
            .thenReturn(Arrays.asList(hostNameA, hostNameB));
    ketamaClient.updateDNS();

    assertTrue(ketamaClient.isConnected());
    assertTrue(knownClients.get(hostNameA).isConnected());
    assertTrue(knownClients.get(hostNameB).isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
            .thenReturn(Arrays.asList(hostNameB, hostNameC));
    ketamaClient.updateDNS();

    assertTrue(ketamaClient.isConnected());
    assertFalse(knownClients.get(hostNameA).isConnected());
    assertTrue(knownClients.get(hostNameB).isConnected());
    assertTrue(knownClients.get(hostNameC).isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
            .thenReturn(Arrays.asList(hostNameC, hostNameD));
    ketamaClient.updateDNS();

    assertTrue(ketamaClient.isConnected());
    assertFalse(knownClients.get(hostNameA).isConnected());
    assertFalse(knownClients.get(hostNameB).isConnected());
    assertTrue(knownClients.get(hostNameC).isConnected());
    assertTrue(knownClients.get(hostNameD).isConnected());

    ketamaClient.shutdown();
  }
}

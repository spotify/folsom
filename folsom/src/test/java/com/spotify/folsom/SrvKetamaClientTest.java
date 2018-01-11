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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.spotify.dns.LookupResult;
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.folsom.ketama.SrvKetamaClient;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SrvKetamaClientTest {
  @Test
  public void testSimple() throws Exception {
    HostAndPort hostNameA = HostAndPort.fromString("a:1");
    HostAndPort hostNameB = HostAndPort.fromString("b:1");
    HostAndPort hostNameC = HostAndPort.fromString("c:1");
    HostAndPort hostNameD = HostAndPort.fromString("d:1");

    DnsSrvResolver resolver = Mockito.mock(DnsSrvResolver.class);

    // Run shutdown code immediately
    DeterministicScheduler executor = new DeterministicScheduler();

    final Map<HostAndPort, FakeRawMemcacheClient> knownClients = Maps.newHashMap();

    SrvKetamaClient.Connector connector = input -> {
      FakeRawMemcacheClient fakeRawMemcacheClient = new FakeRawMemcacheClient();
      knownClients.put(input, fakeRawMemcacheClient);
      return fakeRawMemcacheClient;
    };

    SrvKetamaClient ketamaClient = new SrvKetamaClient(
            "the-srv-record", resolver, executor, 1000, TimeUnit.MILLISECONDS,
            connector, 1000, TimeUnit.MILLISECONDS);
    executor.tick(1000, TimeUnit.SECONDS);

    assertFalse(ketamaClient.isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
            .thenReturn(ImmutableList.of(result("a"), result("b")));
    ketamaClient.updateDNS();
    executor.tick(1000, TimeUnit.SECONDS);

    assertTrue(ketamaClient.isConnected());
    assertTrue(knownClients.get(hostNameA).isConnected());
    assertTrue(knownClients.get(hostNameB).isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
            .thenReturn(ImmutableList.of(result("b"), result("c")));
    ketamaClient.updateDNS();
    executor.tick(1000, TimeUnit.SECONDS);

    assertTrue(ketamaClient.isConnected());
    assertFalse(knownClients.get(hostNameA).isConnected());
    assertTrue(knownClients.get(hostNameB).isConnected());
    assertTrue(knownClients.get(hostNameC).isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
        .thenReturn(ImmutableList.of(result("c"), result("d")));
    ketamaClient.updateDNS();
    executor.tick(1000, TimeUnit.SECONDS);

    assertTrue(ketamaClient.isConnected());
    assertFalse(knownClients.get(hostNameA).isConnected());
    assertFalse(knownClients.get(hostNameB).isConnected());
    assertTrue(knownClients.get(hostNameC).isConnected());
    assertTrue(knownClients.get(hostNameD).isConnected());

    ketamaClient.shutdown();
  }

  private LookupResult result(final String hostname) {
    return LookupResult.create(hostname, 1, 100, 100, 100);
  }
}

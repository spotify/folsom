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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.LookupResult;
import com.spotify.folsom.client.test.FakeRawMemcacheClient;
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.folsom.ketama.ResolvingKetamaClient;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;
import org.mockito.Mockito;

public class ResolvingKetamaClientTest {
  @Test
  public void testSimple() throws Exception {
    HostAndPort hostNameA = HostAndPort.fromString("a:1");
    HostAndPort hostNameB = HostAndPort.fromString("b:1");
    HostAndPort hostNameC = HostAndPort.fromString("c:1");
    HostAndPort hostNameD = HostAndPort.fromString("d:1");

    DnsSrvResolver resolver = Mockito.mock(DnsSrvResolver.class);

    // Run shutdown code immediately
    DeterministicScheduler executor = new DeterministicScheduler();

    final Map<HostAndPort, FakeRawMemcacheClient> knownClients = new HashMap<>();

    ResolvingKetamaClient.Connector connector =
        input -> {
          FakeRawMemcacheClient fakeRawMemcacheClient = new FakeRawMemcacheClient();
          knownClients.put(input, fakeRawMemcacheClient);
          return fakeRawMemcacheClient;
        };

    ResolvingKetamaClient ketamaClient =
        new ResolvingKetamaClient(
            SrvResolver.newBuilder("the-srv-record").withSrvResolver(resolver).build(),
            executor,
            1000,
            TimeUnit.MILLISECONDS,
            connector,
            1000,
            TimeUnit.MILLISECONDS);
    executor.tick(1000, TimeUnit.SECONDS);

    assertFalse(ketamaClient.isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
        .thenReturn(ImmutableList.of(result("a"), result("b")));
    ketamaClient.resolve();
    executor.tick(1000, TimeUnit.SECONDS);

    assertTrue(ketamaClient.isConnected());
    assertTrue(knownClients.get(hostNameA).isConnected());
    assertTrue(knownClients.get(hostNameB).isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
        .thenReturn(ImmutableList.of(result("b"), result("c")));
    ketamaClient.resolve();
    executor.tick(1000, TimeUnit.SECONDS);

    assertTrue(ketamaClient.isConnected());
    assertFalse(knownClients.get(hostNameA).isConnected());
    assertTrue(knownClients.get(hostNameB).isConnected());
    assertTrue(knownClients.get(hostNameC).isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
        .thenReturn(ImmutableList.of(result("c"), result("d")));
    ketamaClient.resolve();
    executor.tick(1000, TimeUnit.SECONDS);

    assertTrue(ketamaClient.isConnected());
    assertFalse(knownClients.get(hostNameA).isConnected());
    assertFalse(knownClients.get(hostNameB).isConnected());
    assertTrue(knownClients.get(hostNameC).isConnected());
    assertTrue(knownClients.get(hostNameD).isConnected());

    // Ignore empty dns results
    Mockito.when(resolver.resolve(Mockito.anyString())).thenReturn(ImmutableList.of());
    ketamaClient.resolve();
    executor.tick(1000, TimeUnit.SECONDS);

    assertTrue(ketamaClient.isConnected());
    assertFalse(knownClients.get(hostNameA).isConnected());
    assertFalse(knownClients.get(hostNameB).isConnected());
    assertTrue(knownClients.get(hostNameC).isConnected());
    assertTrue(knownClients.get(hostNameD).isConnected());

    ketamaClient.shutdown();
  }

  @Test
  public void testReusingConnections() {
    DnsSrvResolver resolver = Mockito.mock(DnsSrvResolver.class);

    // Run shutdown code immediately
    DeterministicScheduler executor = new DeterministicScheduler();

    Set<RawMemcacheClient> allClients = Collections.newSetFromMap(new IdentityHashMap<>());

    final Map<HostAndPort, FakeRawMemcacheClient> knownClients = new HashMap<>();

    ResolvingKetamaClient.Connector connector =
        input -> {
          FakeRawMemcacheClient fakeRawMemcacheClient = new FakeRawMemcacheClient();
          allClients.add(fakeRawMemcacheClient);
          knownClients.put(input, fakeRawMemcacheClient);
          return fakeRawMemcacheClient;
        };

    ResolvingKetamaClient ketamaClient =
        new ResolvingKetamaClient(
            SrvResolver.newBuilder("the-srv-record").withSrvResolver(resolver).build(),
            executor,
            1000,
            TimeUnit.MILLISECONDS,
            connector,
            1000,
            TimeUnit.MILLISECONDS);
    executor.tick(1000, TimeUnit.SECONDS);

    assertFalse(ketamaClient.isConnected());

    Mockito.when(resolver.resolve(Mockito.anyString()))
        .thenReturn(ImmutableList.of(result("a"), result("b")));
    ketamaClient.resolve();
    assertEquals(2, allClients.size());
    executor.tick(1000, TimeUnit.SECONDS);

    Mockito.when(resolver.resolve(Mockito.anyString()))
        .thenReturn(ImmutableList.of(result("b"), result("c")));
    ketamaClient.resolve();
    assertEquals(3, allClients.size());
    executor.tick(1000, TimeUnit.SECONDS);

    Mockito.when(resolver.resolve(Mockito.anyString()))
        .thenReturn(ImmutableList.of(result("c"), result("d")));
    ketamaClient.resolve();
    assertEquals(4, allClients.size());
    executor.tick(1000, TimeUnit.SECONDS);
  }

  private LookupResult result(final String hostname) {
    return LookupResult.create(hostname, 1, 100, 100, 100);
  }
}

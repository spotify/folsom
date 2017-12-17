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
package com.spotify.folsom.roundrobin;

import com.google.common.base.Charsets;
import com.spotify.folsom.FakeRawMemcacheClient;
import com.spotify.folsom.MemcacheClosedException;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.transcoder.StringTranscoder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RoundRobinMemcacheClientTest {

  private FakeRawMemcacheClient client1;
  private FakeRawMemcacheClient client2;
  private FakeRawMemcacheClient client3;
  private List<RawMemcacheClient> clientList;
  private RoundRobinMemcacheClient roundRobinMemcacheClient;
  private DefaultAsciiMemcacheClient<String> memcacheClient;

  @Before
  public void setUp() throws Exception {
    client1 = new FakeRawMemcacheClient();
    client2 = new FakeRawMemcacheClient();
    client3 = new FakeRawMemcacheClient();
    clientList = Arrays.<RawMemcacheClient>asList(client1, client2, client3);
    roundRobinMemcacheClient = new RoundRobinMemcacheClient(clientList);
    memcacheClient = new DefaultAsciiMemcacheClient<>(
            roundRobinMemcacheClient, new NoopMetrics(),
            StringTranscoder.UTF8_INSTANCE, Charsets.UTF_8,
        MemcacheEncoder.MAX_KEY_LEN);
  }

  @Test
  public void testShutdown() throws Exception {
    for (RawMemcacheClient client : clientList) {
      assertEquals(true, client.isConnected());
    }
    roundRobinMemcacheClient.shutdown();
    for (RawMemcacheClient client : clientList) {
      assertEquals(false, client.isConnected());
    }
  }

  @Test
  public void testDistribution() throws Exception {
    for (int i = 0; i < clientList.size() * 1000; i++) {
      memcacheClient.set("key" + i, "value" + i, 0).toCompletableFuture().get();
    }

    assertEquals(1000, client1.getMap().size());
    assertEquals(1000, client2.getMap().size());
    assertEquals(1000, client3.getMap().size());

    for (int i = 0; i < clientList.size() * 1000; i++) {
      assertEquals("value" + i, memcacheClient.get("key" + i).toCompletableFuture().get());
    }
  }

  @Test
  public void testAvoidDisconnected() throws Exception {
    for (int i = 0; i < 3000; i++) {
      memcacheClient.set("key" + i, "value" + i, 0).toCompletableFuture().get();
    }

    assertEquals(1000, client1.getMap().size());
    assertEquals(1000, client2.getMap().size());
    assertEquals(1000, client3.getMap().size());

    assertTrue(memcacheClient.isConnected());
    client2.shutdown();

    for (int i = 0; i < 3000; i++) {
      memcacheClient.set("key2-" + i, "value2-" + i, 0).toCompletableFuture().get();
    }

    assertEquals(2500, client1.getMap().size());
    assertEquals(1000, client2.getMap().size());
    assertEquals(2500, client3.getMap().size());

    client1.shutdown();
    client3.shutdown();
    assertFalse(memcacheClient.isConnected());
  }

  @Test(expected = MemcacheClosedException.class)
  public void testAllDisconnected() throws Throwable {
    client1.shutdown();
    client2.shutdown();
    client3.shutdown();
    try {
      memcacheClient.set("key", "value", 0).toCompletableFuture().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }
}

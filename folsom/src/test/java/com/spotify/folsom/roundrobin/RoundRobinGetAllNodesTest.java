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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.spotify.folsom.AsciiMemcacheClient;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.NoopTracer;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.client.test.FakeRawMemcacheClient;
import com.spotify.folsom.transcoder.StringTranscoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class RoundRobinGetAllNodesTest {

  private FakeRawMemcacheClient client1;
  private FakeRawMemcacheClient client2;
  private FakeRawMemcacheClient client3;
  private List<RawMemcacheClient> clientList;
  private RoundRobinMemcacheClient roundRobinMemcacheClient;
  private DefaultAsciiMemcacheClient<String> memcacheClient;

  @Before
  public void setUp() throws Exception {
    client1 = new FakeRawMemcacheClient(new NoopMetrics(), "address1:123");
    client2 = new FakeRawMemcacheClient(new NoopMetrics(), "address2:123");
    client3 = new FakeRawMemcacheClient(new NoopMetrics(), "address3:123");
    clientList = Arrays.<RawMemcacheClient>asList(client1, client2, client3);
    roundRobinMemcacheClient = new RoundRobinMemcacheClient(clientList);
    memcacheClient =
        new DefaultAsciiMemcacheClient<>(
            roundRobinMemcacheClient,
            new NoopMetrics(),
            NoopTracer.INSTANCE,
            StringTranscoder.UTF8_INSTANCE,
            StandardCharsets.UTF_8,
            MemcacheEncoder.MAX_KEY_LEN);
  }

  @Test
  public void testGetAllNodesDifferentAddresses() {
    memcacheClient.set("key", "value1", 0).toCompletableFuture().join();
    memcacheClient.set("key", "value2", 0).toCompletableFuture().join();
    memcacheClient.set("key", "value3", 0).toCompletableFuture().join();
    final Map<String, AsciiMemcacheClient<String>> nodes = memcacheClient.getAllNodes();

    assertEquals(3, nodes.size());
    assertNode(nodes, "address1:123", "key", "value3");
    assertNode(nodes, "address2:123", "key", "value1");
    assertNode(nodes, "address3:123", "key", "value2");
  }

  private void assertNode(
      Map<String, AsciiMemcacheClient<String>> nodes, String address, String key, String value) {
    final AsciiMemcacheClient<String> client = nodes.get(address);
    assertNotNull(client);
    final String actualValue = client.get(key).toCompletableFuture().join();
    assertEquals(value, actualValue);
  }
}

/*
 * Copyright (c) 2014-2015 Spotify AB
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
package com.spotify.folsom.ketama;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.spotify.folsom.guava.HostAndPort;

import com.spotify.folsom.FakeRawMemcacheClient;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.client.binary.DefaultBinaryMemcacheClient;
import com.spotify.folsom.transcoder.StringTranscoder;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KetamaMemcacheClientTest {

  @Test
  public void testStitching() throws Exception {
    for (int i = 0; i < 2; i++) {
      boolean binary = i == 0;
      test(4, 100, 20, binary);
      test(4, 5, 50, binary);
      test(4, 100, 0, binary);
      test(4, 100, 1, binary);
      test(4, 100, 2, binary);
      test(4, 100, 3, binary);
      test(4, 100, 4, binary);
    }
  }

  private void test(final int numClients, final int keysFound, final int requestSize,
                    boolean binary)
      throws InterruptedException, ExecutionException {

    final Random random = new Random(123717678612L);

    final ArrayList<AddressAndClient> clients = Lists.newArrayList();
    for (int i = 0; i < numClients; i++) {
      final FakeRawMemcacheClient client = new FakeRawMemcacheClient();
      final AddressAndClient aac = new AddressAndClient(
          HostAndPort.fromString("host" + i + ":11211"), client);
      clients.add(aac);

      final MemcacheClient<String> memcacheClient = buildClient(client, binary);

      for (int j = 0; j < keysFound; j++) {
        memcacheClient.set("key-" + j, "value-" + j + "-" + i, 1000).toCompletableFuture().get();
      }
    }

    final KetamaMemcacheClient ketamaMemcacheClient = new KetamaMemcacheClient(clients);
    final MemcacheClient<String> memcacheClient = buildClient(ketamaMemcacheClient, binary);

    final List<String> requestedKeys = Lists.newArrayList();
    for (int i = 0; i < requestSize; i++) {
      requestedKeys.add("key-" + random.nextInt(keysFound * 2));
    }
    final List<String> values = memcacheClient.get(requestedKeys).toCompletableFuture().get();
    assertEquals(requestSize, values.size());

    for (int i = 0; i < requestSize; i++) {
      final String key = requestedKeys.get(i);
      final String value = values.get(i);
      final String keyIndex = Splitter.on('-').splitToList(key).get(1);
      if (Integer.parseInt(keyIndex) >= keysFound) {
        assertNull(value);
      } else {
        assertNotNull(value);
        final List<String> parts = Splitter.on('-').splitToList(value);
        assertEquals(3, parts.size());
        assertEquals("value", parts.get(0));
        assertEquals(keyIndex, parts.get(1));
      }
    }
  }

  private MemcacheClient<String> buildClient(final RawMemcacheClient client, boolean binary) {
    if (binary) {
      return new DefaultBinaryMemcacheClient<>(client, new NoopMetrics(),
              StringTranscoder.UTF8_INSTANCE, Charsets.UTF_8, MemcacheEncoder.MAX_KEY_LEN);
    } else {
      return new DefaultAsciiMemcacheClient<>(client, new NoopMetrics(),
              StringTranscoder.UTF8_INSTANCE, Charsets.UTF_8, MemcacheEncoder.MAX_KEY_LEN);
    }
  }
}

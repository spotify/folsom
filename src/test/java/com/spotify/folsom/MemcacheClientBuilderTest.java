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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import java.util.concurrent.CompletionStage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MemcacheClientBuilderTest {

  private EmbeddedServer server;

  @Before
  public void setUp() throws Exception {
    server = new EmbeddedServer(false);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void testValidLatin1() throws Exception {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withKeyCharset(Charsets.ISO_8859_1)
            .withAddress(HostAndPort.fromParts("127.0.0.6", server.getPort()))
            .connectAscii();
    try {
      ConnectFuture.connectFuture(client).toCompletableFuture().get();
      assertEquals(null, client.get("Räksmörgås").toCompletableFuture().get());
    } finally {
      client.shutdown();
      ConnectFuture.disconnectFuture(client).toCompletableFuture().get();
    }
  }

  @Test
  public void testValidUTF8() throws Exception {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withKeyCharset(Charsets.UTF_8)
            .withAddress(HostAndPort.fromParts("127.0.0.7", server.getPort()))
            .connectAscii();
    try {
      ConnectFuture.connectFuture(client).toCompletableFuture().get();
      assertEquals(null, client.get("Räksmörgås").toCompletableFuture().get());
    } finally {
      client.shutdown();
      ConnectFuture.disconnectFuture(client).toCompletableFuture().get();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUTF16() throws Exception {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withKeyCharset(Charsets.UTF_16)
            .withAddress(HostAndPort.fromParts("127.0.0.3", server.getPort()))
            .connectAscii();
    try {
      ConnectFuture.connectFuture(client).toCompletableFuture().get();
      client.get("Key").toCompletableFuture().get();
    } finally {
      client.shutdown();
      ConnectFuture.disconnectFuture(client).toCompletableFuture().get();
    }
  }

  @Test(expected = MemcacheOverloadedException.class)
  public void testOverloaded() throws Throwable {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withAddress(HostAndPort.fromParts("127.0.0.5", server.getPort()))
            .withMaxOutstandingRequests(100)
            .connectAscii();
    ConnectFuture.connectFuture(client).toCompletableFuture().get();

    try {
      List<CompletionStage<String>> futures = Lists.newArrayList();
      for (int i = 0; i < 400; i++) {
        futures.add(client.get("key"));
      }
      for (CompletionStage<String> future : futures) {
        try {
          future.toCompletableFuture().get();
        } catch (ExecutionException e) {
          throw e.getCause();
        }
      }
      fail("No MemcacheOverloadedException was triggered");
    } finally {
      client.shutdown();
      ConnectFuture.disconnectFuture(client).toCompletableFuture().get();
    }
  }

  @Test
  public void testMaxSetLength() throws Throwable {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withAddress(HostAndPort.fromParts("127.0.0.4", server.getPort()))
            .withMaxSetLength(1)
            .connectAscii();
    ConnectFuture.connectFuture(client).toCompletableFuture().get();

    try {
      assertEquals(
          MemcacheStatus.VALUE_TOO_LARGE,
          client.set("key", "value", 100).toCompletableFuture().get());
      assertEquals(null, client.get("key").toCompletableFuture().get());
    } finally {
      client.shutdown();
      ConnectFuture.disconnectFuture(client).toCompletableFuture().get();
    }
  }

}

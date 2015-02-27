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
package com.spotify.folsom.client;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.ConnectFuture;
import com.spotify.folsom.EmbeddedServer;
import com.spotify.folsom.MemcacheClosedException;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.ascii.AsciiRequest;
import com.spotify.folsom.client.ascii.AsciiResponse;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.client.ascii.GetRequest;
import com.spotify.folsom.transcoder.StringTranscoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DefaultRawMemcacheClientTest {
  private static EmbeddedServer embeddedServer;
  private static HostAndPort embeddedAddress;

  @BeforeClass
  public static void setUp() throws Exception {
    embeddedServer = new EmbeddedServer(false);
    embeddedAddress = HostAndPort.fromParts("localhost", embeddedServer.getPort());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    embeddedServer.stop();
  }

  @Test
  public void testInvalidRequest() throws Exception {
    final String exceptionString = "Crash the client";

    RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        embeddedAddress, 5000, false, null, 3000, Charsets.UTF_8).get();

    DefaultAsciiMemcacheClient<String> asciiClient =
            new DefaultAsciiMemcacheClient<>(
                    rawClient, new NoopMetrics(),
                    new StringTranscoder(Charsets.UTF_8), Charsets.UTF_8);

    List<ListenableFuture<?>> futures = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      futures.add(asciiClient.set("key", "value" + i, 0));
    }

    sendFailRequest(exceptionString, rawClient);

    for (int i = 0; i < 2; i++) {
      futures.add(asciiClient.set("key", "value" + i, 0));
    }

    assertFalse(rawClient.isConnected());

    int total = futures.size();
    int stuck = 0;

    StringBuilder sb = new StringBuilder();
    long t1 = System.currentTimeMillis();
    int i = 0;
    for (ListenableFuture<?> future : futures) {
      try {
        long elapsed = System.currentTimeMillis() - t1;
        future.get(Math.max(0, 1000 - elapsed), TimeUnit.MILLISECONDS);
        sb.append('.');
      } catch (ExecutionException e) {
        assertEquals(MemcacheClosedException.class, e.getCause().getClass());
        sb.append('.');
      } catch (TimeoutException e) {
        sb.append('X');
        stuck++;
      }
      i++;
      if (0 == (i % 50)) {
        sb.append("\n");
      }
    }
    assertEquals(stuck + " out of " + total + " requests got stuck:\n" + sb.toString(), 0, stuck);
  }

  private void sendFailRequest(final String exceptionString, RawMemcacheClient rawClient) throws InterruptedException {
    try {
      rawClient.send(new AsciiRequest<String>("key", Charsets.UTF_8) {
        @Override
        protected void handle(AsciiResponse response) throws IOException {
          throw new IOException(exceptionString);
        }

        @Override
        public ByteBuf writeRequest(ByteBufAllocator alloc, ByteBuffer dst) {
          dst.put("invalid command".getBytes());
          dst.put(NEWLINE_BYTES);
          return toBuffer(alloc, dst);
        }
      }).get();
      fail();
    } catch (ExecutionException e) {
      assertEquals("Unexpected line: CLIENT_ERROR", e.getCause().getMessage());
    }
  }

  @Test
  public void testRequestTimeout() throws IOException, ExecutionException, InterruptedException {
    final ServerSocket server = new ServerSocket();
    server.bind(null);

    final HostAndPort address = HostAndPort.fromParts("127.0.0.1", server.getLocalPort());
    RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        address, 5000, false, null, 1000, Charsets.UTF_8).get();

    final Future<?> future = rawClient.send(new GetRequest("foo", Charsets.UTF_8, false));
    try {
      future.get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof MemcacheClosedException);
    }
  }

  @Test
  public void testCancelledWriteRequest() throws Throwable {

    DefaultRawMemcacheClient rawClient = (DefaultRawMemcacheClient) DefaultRawMemcacheClient.connect(
            embeddedAddress, 5000, false, null, 1000, Charsets.UTF_8).get();

    DefaultAsciiMemcacheClient<String> asciiClient = new DefaultAsciiMemcacheClient<>(
            rawClient, NoopMetrics.INSTANCE, StringTranscoder.UTF8_INSTANCE, Charsets.UTF_8);

    ConnectFuture.connectFuture(asciiClient).get();

    try {
      for (int i = 0; i < 100; i++) {
        List<ListenableFuture<?>> futures = Lists.newArrayList();
        for (int j = 0; j < 100; j++) {
          ListenableFuture<String> future = asciiClient.get("key");
          future.cancel(true);
          futures.add(future);
        }
        for (ListenableFuture<?> future : futures) {
          try {
            future.get();
          } catch (CancellationException e) {
            // Ignore this, it's expected
          }
        };
        if (rawClient.numSkippedWrites() > 0) {
          return;
        }

      }
      fail("Could not trigger a skip of writes");
    } finally {
      asciiClient.shutdown();
    }
  }

  @Test
  public void testCancelledReadRequest() throws Throwable {

    DefaultRawMemcacheClient rawClient = (DefaultRawMemcacheClient) DefaultRawMemcacheClient.connect(
            embeddedAddress, 50000, false, null, 1000, Charsets.UTF_8).get();

    DefaultAsciiMemcacheClient<String> asciiClient = new DefaultAsciiMemcacheClient<>(
            rawClient, NoopMetrics.INSTANCE, StringTranscoder.UTF8_INSTANCE, Charsets.UTF_8);

    ConnectFuture.connectFuture(asciiClient).get();

    try {
      for (int i = 0; i < 100; i++) {
        List<ListenableFuture<?>> futures = Lists.newArrayList();
        for (int j = 0; j < 100; j++) {
          futures.add(asciiClient.get("key"));
        }
        Thread.sleep(1);
        for (ListenableFuture<?> future : futures) {
          future.cancel(true);
        }
        for (ListenableFuture<?> future : futures) {
          try {
            future.get();
          } catch (CancellationException e) {
            // Ignore this, it's expected
          }
        };
        if (rawClient.numSkippedReads() > 0) {
          return;
        }

      }
      fail("Could not trigger a skip of reads");
    } finally {
      asciiClient.shutdown();
    }
  }

  @Test
  public void testShutdown() throws IOException, ExecutionException, InterruptedException {
    final ServerSocket server = new ServerSocket();
    server.bind(null);

    final HostAndPort address = HostAndPort.fromParts("127.0.0.1", server.getLocalPort());
    RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        address, 5000, false, null, 1000, Charsets.UTF_8).get();

    rawClient.shutdown();
    ConnectFuture.disconnectFuture(rawClient).get();
    assertFalse(rawClient.isConnected());
  }
}
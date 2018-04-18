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
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.futures.CompletableFutures;
import java.util.concurrent.CompletionStage;
import com.google.common.util.concurrent.MoreExecutors;

import com.spotify.folsom.EmbeddedServer;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheClosedException;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.ascii.AsciiRequest;
import com.spotify.folsom.client.ascii.AsciiResponse;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.client.ascii.GetRequest;
import com.spotify.folsom.transcoder.StringTranscoder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import static com.spotify.folsom.client.Utils.unwrap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DefaultRawMemcacheClientTest {

  private EmbeddedServer asciiServer;
  private EmbeddedServer binaryServer;

  @Before
  public void setUpBinaryServer() throws Exception {
    binaryServer = new EmbeddedServer(true);
  }

  @Before
  public void setUpAsciiServer() throws Exception {
    asciiServer = new EmbeddedServer(false);
  }

  @After
  public void tearDownAsciiServer() throws Exception {
    asciiServer.stop();
  }

  @After
  public void tearDownBinaryServer() throws Exception {
    binaryServer.stop();
  }

  @Test
  public void testInvalidRequest() throws Exception {
    final String exceptionString = "Crash the client";

    RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        HostAndPort.fromParts("127.0.0.11", asciiServer.getPort()),
        5000,
        false,
        null,
        3000,
        Charsets.UTF_8,
        new NoopMetrics(), 1024 * 1024,
        null,
        null
    ).toCompletableFuture().get();

    DefaultAsciiMemcacheClient<String> asciiClient =
            new DefaultAsciiMemcacheClient<>(
                    rawClient, new NoopMetrics(),
                    new StringTranscoder(Charsets.UTF_8), Charsets.UTF_8,
                MemcacheEncoder.MAX_KEY_LEN);

    try {
      List<CompletionStage<?>> futures = Lists.newArrayList();
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
      for (CompletionStage<?> future : futures) {
        try {
          long elapsed = System.currentTimeMillis() - t1;
          future.toCompletableFuture().get(Math.max(0, 1000 - elapsed), TimeUnit.MILLISECONDS);
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
    } finally {
      asciiClient.shutdown();
      asciiClient.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  private void sendFailRequest(final String exceptionString, RawMemcacheClient rawClient)
      throws InterruptedException {
    try {
      rawClient.send(new AsciiRequest<String>("key".getBytes(Charsets.UTF_8)) {
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
      }).toCompletableFuture().get();
      fail();
    } catch (ExecutionException e) {
      assertEquals("Unexpected line: CLIENT_ERROR", e.getCause().getMessage());
    }
  }

  @Test
  public void testRequestTimeout() throws Exception {
    final ServerSocket server = new ServerSocket();
    server.bind(null);

    final HostAndPort address = HostAndPort.fromParts("127.0.0.1", server.getLocalPort());
    RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        address, 5000, false, null, 1000,
            Charsets.UTF_8, new NoopMetrics(), 1024 * 1024, null, null)
        .toCompletableFuture().get();

    final CompletionStage<?> future = rawClient.send(
        new GetRequest("foo".getBytes(Charsets.UTF_8), false));
    try {
      future.toCompletableFuture().get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(unwrap(e) instanceof MemcacheClosedException);
      rawClient.shutdown();
      rawClient.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testBinaryRequestRetry() throws Exception {
    final int outstandingRequestLimit = 5000;
    final boolean binary = true;
    final Executor executor = MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) Executors.newCachedThreadPool());
    final int timeoutMillis = 3000;
    final int maxSetLength = 1024 * 1024;

    final HostAndPort address = HostAndPort.fromParts("127.0.0.10", binaryServer.getPort());
    final RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        address, outstandingRequestLimit, binary, executor, timeoutMillis, Charsets.UTF_8,
        new NoopMetrics(), maxSetLength, null, null).toCompletableFuture().get();

    try {
      byte[] key = "foo".getBytes(Charsets.UTF_8);
      final com.spotify.folsom.client.binary.GetRequest request =
          new com.spotify.folsom.client.binary.GetRequest(key, OpCode.GET, 123);

      // Send request once
      rawClient.send(request).toCompletableFuture().get();

      binaryServer.flush();

      // Pretend that the above request failed and retry it by sending it again
      rawClient.send(request).toCompletableFuture().get();
    } finally {
      rawClient.shutdown();
      rawClient.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testAsciiRequestRetry() throws Exception {
    final boolean binary = false;

    final int outstandingRequestLimit = 5000;
    final Executor executor = MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) Executors.newCachedThreadPool());
    final int timeoutMillis = 3000;
    final int maxSetLength = 1024 * 1024;

    final HostAndPort address = HostAndPort.fromParts("127.0.0.9", asciiServer.getPort());
    final RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        address, outstandingRequestLimit, binary, executor, timeoutMillis, Charsets.UTF_8,
        new NoopMetrics(), maxSetLength, null, null).toCompletableFuture().get();

    try {
      final GetRequest request =
          new GetRequest("foo".getBytes(Charsets.UTF_8), false);

      // Send request once
      rawClient.send(request).toCompletableFuture().get();

      asciiServer.flush();

      // Pretend that the above request failed and retry it by sending it again
      rawClient.send(request).toCompletableFuture().get();
    } finally {
      rawClient.shutdown();
      rawClient.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testShutdown() throws Exception {
    final ServerSocket server = new ServerSocket();
    server.bind(null);

    final HostAndPort address = HostAndPort.fromParts("127.0.0.1", server.getLocalPort());
    RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        address, 5000, false, null, 1000,
            Charsets.UTF_8, new NoopMetrics(), 1024 * 1024, null, null)
        .toCompletableFuture().get();

    rawClient.shutdown();
    rawClient.awaitDisconnected(10, TimeUnit.SECONDS);
    assertFalse(rawClient.isConnected());
  }

  @Test(expected = MemcacheClosedException.class)
  public void testShutdownRequestExceptionInsteadOfOverloaded() throws Throwable {
    final ServerSocket server = new ServerSocket();
    server.bind(null);

    final HostAndPort address = HostAndPort.fromParts("127.0.0.1", server.getLocalPort());
    RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        address, 1, false, null, 1000, Charsets.UTF_8, new NoopMetrics(), 1024 * 1024, null, null)
        .toCompletableFuture().get();

    rawClient.shutdown();
    rawClient.awaitDisconnected(10, TimeUnit.SECONDS);
    assertFalse(rawClient.isConnected());

    // Try to fill up the outstanding request limit
    for (int i = 0; i < 100; i++) {
      try {
        rawClient.send(new GetRequest("key".getBytes(Charsets.UTF_8), false))
            .toCompletableFuture().get();
      } catch (ExecutionException e) {
        // Ignore any errors here
      }
    }

    try {
      rawClient.send(new GetRequest("key".getBytes(Charsets.UTF_8), false))
          .toCompletableFuture().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = MemcacheClosedException.class)
  public void testShutdownRequestException() throws Throwable {
    final ServerSocket server = new ServerSocket();
    server.bind(null);

    final HostAndPort address = HostAndPort.fromParts("127.0.0.1", server.getLocalPort());
    RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
        address, 1, false, null, 1000, Charsets.UTF_8, new NoopMetrics(), 1024 * 1024, null, null)
        .toCompletableFuture().get();

    rawClient.shutdown();
    rawClient.awaitDisconnected(10, TimeUnit.SECONDS);
    assertFalse(rawClient.isConnected());

    try {
      rawClient.send(
          new GetRequest("key".getBytes(Charsets.UTF_8), false)).toCompletableFuture().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testOutstandingRequestMetric() throws Exception {
    final OutstandingRequestListenerMetrics metrics = new OutstandingRequestListenerMetrics();
    final Charset charset = Charsets.UTF_8;
    final byte[] response = "END\r\n".getBytes(charset);

    try (SlowStaticServer server = new SlowStaticServer(response, 1000)) {

      final int port = server.start(0);

      RawMemcacheClient rawClient = DefaultRawMemcacheClient.connect(
          HostAndPort.fromParts("127.0.0.12", port),
          5000,
          false,
          null,
          3000,
          charset,
          metrics,
          1024 * 1024,
          null,
          null
      ).toCompletableFuture().get();

      try {
        assertNotNull(metrics.getGauge());
        assertEquals(0, metrics.getGauge().getOutstandingRequests());

        List<CompletionStage<GetResult<byte[]>>> futures = new ArrayList<>();
        futures.add(rawClient.send(new GetRequest("key".getBytes(charset), false)));
        assertEquals(1, metrics.getGauge().getOutstandingRequests());

        futures.add(rawClient.send(new GetRequest("key".getBytes(charset), false)));
        assertEquals(2, metrics.getGauge().getOutstandingRequests());

        // ensure that counter goes back to zero after request is done
        CompletableFutures.allAsList(futures).get(5, TimeUnit.SECONDS);
        assertEquals(0, metrics.getGauge().getOutstandingRequests());
      } finally {
        rawClient.shutdown();
        rawClient.awaitDisconnected(10, TimeUnit.SECONDS);
      }
    }
  }

  private static class OutstandingRequestListenerMetrics extends NoopMetrics {

    private OutstandingRequestsGauge gauge;

    @Override
    public void registerOutstandingRequestsGauge(OutstandingRequestsGauge gauge) {
      this.gauge = gauge;
    }

    public OutstandingRequestsGauge getGauge() {
      return this.gauge;
    }
  }
}

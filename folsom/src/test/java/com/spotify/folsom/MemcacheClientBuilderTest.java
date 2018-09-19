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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.TimeoutException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MemcacheClientBuilderTest {

  private static MemcachedServer server;

  @BeforeClass
  public static void setUp() throws Exception {
    server = new MemcachedServer();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    server.stop();
  }

  @Before
  public void setUpInstance() throws Exception {
    server.flush();
  }

  @Test
  public void testValidLatin1() throws Exception {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withKeyCharset(Charsets.ISO_8859_1)
            .withAddress(server.getHost(), server.getPort())
            .connectAscii();
    try {
      client.awaitConnected(10, TimeUnit.SECONDS);
      assertNull(client.get("Räksmörgås").toCompletableFuture().get());
    } finally {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testValidUTF8() throws Exception {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withKeyCharset(Charsets.UTF_8)
            .withAddress(server.getHost(), server.getPort())
            .connectAscii();
    try {
      client.awaitConnected(10, TimeUnit.SECONDS);
      assertNull(client.get("Räksmörgås").toCompletableFuture().get());
    } finally {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUTF16() throws Exception {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withKeyCharset(Charsets.UTF_16)
            .withAddress(server.getHost(), server.getPort())
            .connectAscii();
    try {
      client.awaitConnected(10, TimeUnit.SECONDS);
      client.get("Key").toCompletableFuture().get();
    } finally {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  @Test(expected = MemcacheOverloadedException.class)
  public void testOverloaded() throws Throwable {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withAddress(server.getHost(), server.getPort())
            .withMaxOutstandingRequests(100)
            .connectAscii();
    client.awaitConnected(10, TimeUnit.SECONDS);

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
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testMaxSetLength() throws Throwable {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withAddress(server.getHost(), server.getPort())
            .withMaxSetLength(1)
            .connectAscii();
    client.awaitConnected(10, TimeUnit.SECONDS);

    try {
      assertEquals(
          MemcacheStatus.VALUE_TOO_LARGE,
          client.set("key", "value", 100).toCompletableFuture().get());
      assertNull(client.get("key").toCompletableFuture().get());
    } finally {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testShouldExecuteInEventLoopGroup() throws Exception {
    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withAddress(server.getHost(), server.getPort())
            .withReplyExecutor(null)
            .connectAscii();
    client.awaitConnected(10, TimeUnit.SECONDS);

    try {
      assertExecutesOnThread(client, "defaultRawMemcacheClient");
    } finally {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testShouldExecuteInProvidedEventLoopGroup() throws Exception {
    ThreadFactory factory = new DefaultThreadFactory("provided_elg", true);
    EventLoopGroup elg = new NioEventLoopGroup(0, factory);

    AsciiMemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withAddress(server.getHost(), server.getPort())
            .withReplyExecutor(null)
            .withEventLoopGroup(elg)
            .connectAscii();
    client.awaitConnected(10, TimeUnit.SECONDS);

    try {
      assertExecutesOnThread(client, "provided_elg");
    } finally {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }


  /**
   * Observe a CF callback executing on an expected thread pool.
   *
   * Attempts to defeat the inherent raciness by trying until successful.
   */
  private void assertExecutesOnThread(AsciiMemcacheClient<String> client,
      String expectedThreadNamePrefix)
      throws InterruptedException, ExecutionException, TimeoutException {

    final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);

    while (true) {
      if (System.nanoTime() > deadlineNanos) {
        throw new TimeoutException("Failed to see callback running on ELG thread");
      }

      final Thread thread = client.set("key", "value", 100)
          .thenApply(r -> Thread.currentThread())
          .toCompletableFuture().get(10, TimeUnit.SECONDS);

      if (thread.getName().startsWith(expectedThreadNamePrefix)) {
        // Function ran on the expected thread, success!
        return;
      }

      assertTrue("Callback ran on unexpected thread: " + thread, thread == Thread.currentThread());

      // We lost the race, the future was already completed when thenApply was called. Try again.
    }
  }
}

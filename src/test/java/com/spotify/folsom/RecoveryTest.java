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

package com.spotify.folsom;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import java.util.concurrent.CompletionStage;

import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Utils;
import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;

import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.netty.util.CharsetUtil.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.jboss.netty.buffer.ChannelBuffers.copiedBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RecoveryTest {

  private static final int MAX_OUTSTANDING_REQUESTS = 10;
  private static final int TIMEOUT_MILLIS = 200;

  @Mock Cache<CacheElement> cache;

  private EmbeddedServer server;

  private MemcacheClient<String> client;

  @Before
  public void setUp() throws Exception {
    assertEquals(0, Utils.getGlobalConnectionCount());

    server = new EmbeddedServer(true, cache);
    int port = server.getPort();

    final MemcacheClientBuilder<String> builder = MemcacheClientBuilder.newStringClient()
        .withAddress("127.0.0.1", port)
        .withConnections(1)
        .withMaxOutstandingRequests(MAX_OUTSTANDING_REQUESTS)
        .withMetrics(NoopMetrics.INSTANCE)
        .withRetry(false)
        .withRequestTimeoutMillis(TIMEOUT_MILLIS);

    client = builder.connectBinary();
    client.awaitConnected(10, TimeUnit.SECONDS);
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
    if (server != null) {
      server.stop();
    }

    assertEquals(0, Utils.getGlobalConnectionCount());
  }

  @Test
  public void testOverloadAndTimeoutRecovery() throws Exception {

    // Have memcached block indefinitely on all GET requests
    final GetAnswer answer = new GetAnswer();
    when(cache.get((Key[]) any())).then(answer);

    // Overload the client
    final int overload = 10;
    final List<CompletionStage<String>> overloadFutures = Lists.newArrayList();
    for (int i = 0; i < MAX_OUTSTANDING_REQUESTS + overload; i++) {
      overloadFutures.add(client.get("foo"));
    }
    int timeout = 0;
    int overloaded = 0;
    for (final CompletionStage<String> f : overloadFutures) {
      try {
        f.toCompletableFuture().get();
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof MemcacheOverloadedException) {
          overloaded++;
        } else if (cause instanceof MemcacheClosedException &&
                   cause.getMessage().contains("Timeout")) {
          timeout++;
        }
      }
    }

    // Verify that the expected number of requests failed with MemcacheOverloadedException
    assertThat(overloaded, is(overload));

    // Verify that the rest failed with TimeoutException
    assertThat(timeout, is(MAX_OUTSTANDING_REQUESTS));

    // Wait for the client to reconnect
    client.awaitConnected(10, TimeUnit.SECONDS);

    // Have memcached reply to all GET requests
    answer.set(elements("foo", "bar"));

    // Verify that the client recovers and successfully processes requests
    final List<CompletionStage<String>> recoveryFutures = Lists.newArrayList();
    for (int i = 0; i < MAX_OUTSTANDING_REQUESTS; i++) {
      recoveryFutures.add(client.get("foo"));
    }
    for (final CompletionStage<String> f : recoveryFutures) {
      f.toCompletableFuture().get();
    }
  }

  private CacheElement[] elements(final String... keyValues) {
    final CacheElement[] elements = new CacheElement[keyValues.length / 2];
    for (int i = 0; i < elements.length; i++) {
      elements[i] = element(keyValues[i * 2], keyValues[i * 2 + 1]);
    }
    return elements;
  }

  private LocalCacheElement element(final String key1, final String value1) {
    final LocalCacheElement e = new LocalCacheElement(new Key(copiedBuffer(key1, UTF_8)));
    e.setData(copiedBuffer(value1, UTF_8));
    return e;
  }

  private static class GetAnswer extends AbstractFuture<CacheElement[]>
      implements Answer<CacheElement[]> {

    @Override
    public boolean set(final CacheElement[] value) {
      return super.set(value);
    }

    @Override
    public CacheElement[] answer(final InvocationOnMock invocationOnMock) throws Throwable {
      return get();
    }
  }
}

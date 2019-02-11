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
package com.spotify.folsom.reconnect;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.BackoffFunction;
import com.spotify.folsom.ConnectionChangeListener;
import com.spotify.folsom.EmbeddedServer;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.futures.CompletableFutures;
import com.sun.management.UnixOperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class ReconnectingClientTest {

  private final ScheduledExecutorService scheduledExecutorService =
      mock(ScheduledExecutorService.class);

  @Before
  public void setUp() throws Exception {
    when(scheduledExecutorService.schedule(
            Mockito.<Runnable>any(), anyLong(), Matchers.<TimeUnit>any()))
        .thenAnswer(
            (Answer<Object>)
                invocationOnMock -> {
                  Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
                  runnable.run();
                  return null;
                });
  }

  @Test
  public void testInitialConnect() throws Exception {
    FakeClient delegate = new FakeClient(true, false);

    BackoffFunction backoffFunction = mock(BackoffFunction.class);
    when(backoffFunction.getBackoffTimeMillis(0)).thenReturn(0L);
    when(backoffFunction.getBackoffTimeMillis(1)).thenReturn(123L);

    Connector connector = mock(Connector.class);
    when(connector.connect())
        .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()))
        .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()))
        .thenReturn(CompletableFuture.completedFuture(delegate));

    ReconnectingClient client =
        new ReconnectingClient(
            backoffFunction,
            scheduledExecutorService,
            connector,
            HostAndPort.fromString("localhost:123"));

    verify(connector, times(3)).connect();
    verify(scheduledExecutorService, times(1))
        .schedule(Mockito.<Runnable>any(), eq(0L), eq(TimeUnit.MILLISECONDS));
    verify(scheduledExecutorService, times(1))
        .schedule(Mockito.<Runnable>any(), eq(123L), eq(TimeUnit.MILLISECONDS));
    verify(backoffFunction, times(1)).getBackoffTimeMillis(0);
    verify(backoffFunction, times(1)).getBackoffTimeMillis(1);

    verifyNoMoreInteractions(connector, scheduledExecutorService, backoffFunction);

    assertTrue(client.isConnected());
  }

  @Test
  public void testLostConnectionRetry() throws Exception {
    FakeClient delegate1 = new FakeClient(true, true);
    FakeClient delegate2 = new FakeClient(true, false);

    BackoffFunction backoffFunction = mock(BackoffFunction.class);
    when(backoffFunction.getBackoffTimeMillis(0)).thenReturn(0L);
    when(backoffFunction.getBackoffTimeMillis(1)).thenReturn(123L);

    Connector connector = mock(Connector.class);
    when(connector.connect())
        .thenReturn(CompletableFuture.completedFuture(delegate1))
        .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()))
        .thenReturn(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()))
        .thenReturn(CompletableFuture.completedFuture(delegate2));

    ReconnectingClient client =
        new ReconnectingClient(
            backoffFunction,
            scheduledExecutorService,
            connector,
            HostAndPort.fromString("localhost:123"));

    verify(connector, times(4)).connect();
    verify(scheduledExecutorService, times(1))
        .schedule(Mockito.<Runnable>any(), eq(0L), eq(TimeUnit.MILLISECONDS));
    verify(scheduledExecutorService, times(1))
        .schedule(Mockito.<Runnable>any(), eq(123L), eq(TimeUnit.MILLISECONDS));
    verify(backoffFunction, times(1)).getBackoffTimeMillis(0);
    verify(backoffFunction, times(1)).getBackoffTimeMillis(1);

    verifyNoMoreInteractions(connector, scheduledExecutorService, backoffFunction);

    assertTrue(client.isConnected());
  }

  @Test
  public void testShutdown() throws Exception {
    FakeClient delegate = new FakeClient(true, false);

    BackoffFunction backoffFunction = mock(BackoffFunction.class);
    when(backoffFunction.getBackoffTimeMillis(0)).thenReturn(0L);
    when(backoffFunction.getBackoffTimeMillis(1)).thenReturn(123L);

    Connector connector = mock(Connector.class);
    when(connector.connect()).thenReturn(CompletableFuture.completedFuture(delegate));

    ReconnectingClient client =
        new ReconnectingClient(
            backoffFunction,
            scheduledExecutorService,
            connector,
            HostAndPort.fromString("localhost:123"));

    assertTrue(client.isConnected());
    client.shutdown();
    client.awaitDisconnected(10, TimeUnit.SECONDS);
    assertFalse(client.isConnected());
  }

  private static class FakeClient extends AbstractRawMemcacheClient {

    private boolean connected;
    private boolean disconnectImmediately;

    private FakeClient(boolean connected, boolean disconnectImmediately) {
      this.connected = connected;
      this.disconnectImmediately = disconnectImmediately;
    }

    @Override
    public <T> CompletionStage<T> send(Request<T> request) {
      throw new RuntimeException("Not implemented");
    }

    @Override
    public void shutdown() {
      connected = false;
      notifyConnectionChange();
    }

    @Override
    public boolean isConnected() {
      return connected;
    }

    @Override
    public Throwable getConnectionFailure() {
      return null;
    }

    @Override
    public int numTotalConnections() {
      return 0;
    }

    @Override
    public int numActiveConnections() {
      return 0;
    }

    @Override
    public void registerForConnectionChanges(ConnectionChangeListener listener) {
      super.registerForConnectionChanges(listener);
      if (disconnectImmediately) {
        connected = false;
        notifyConnectionChange();
      }
    }
  }

  @Test
  public void testFiledescriptorLeak() throws InterruptedException, TimeoutException {
    final EmbeddedServer embeddedServer = new EmbeddedServer(false);
    final MemcacheClient<String> client =
        MemcacheClientBuilder.newStringClient()
            .withAddress("localhost", embeddedServer.getPort())
            .connectAscii();
    client.awaitFullyConnected(1, TimeUnit.MINUTES);

    // Warmup (maybe not needed?)
    for (int i = 0; i < 10; i++) {
      triggerReconnect(embeddedServer, client);
    }

    long fdCountPre = getFDCount();
    for (int i = 0; i < 50; i++) {
      triggerReconnect(embeddedServer, client);
    }
    long fdCountPost = getFDCount();

    final long diff = Math.abs(fdCountPost - fdCountPre);
    assertTrue(diff < 10L);
  }

  private void triggerReconnect(
      final EmbeddedServer embeddedServer, final MemcacheClient<String> client)
      throws TimeoutException, InterruptedException {
    embeddedServer.stop();
    client.awaitFullyDisconnected(1, TimeUnit.MINUTES);
    embeddedServer.start();
    client.awaitFullyConnected(1, TimeUnit.MINUTES);
  }

  private long getFDCount() {
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    assumeTrue("Test only supported on Unix based OSes", os instanceof UnixOperatingSystemMXBean);

    return ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
  }
}

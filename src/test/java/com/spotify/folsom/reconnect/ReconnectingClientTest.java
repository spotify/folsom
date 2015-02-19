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

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.BackoffFunction;
import com.spotify.folsom.ConnectionChangeListener;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.Request;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ReconnectingClientTest {

  @Test
  public void testInitialConnect() throws Exception {
    ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
    when(scheduledExecutorService.schedule(Mockito.<Runnable>any(), anyLong(), Matchers.<TimeUnit>any()))
            .thenAnswer(new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
                runnable.run();
                return null;
              }
            });

    FakeClient delegate = new FakeClient(true, false);

    BackoffFunction backoffFunction = mock(BackoffFunction.class);
    when(backoffFunction.getBackoffTimeMillis(0)).thenReturn(0L);
    when(backoffFunction.getBackoffTimeMillis(1)).thenReturn(123L);

    ReconnectingClient.Connector connector = mock(ReconnectingClient.Connector.class);
    when(connector.connect())
            .thenReturn(Futures.<RawMemcacheClient>immediateFailedFuture(new RuntimeException()))
            .thenReturn(Futures.<RawMemcacheClient>immediateFailedFuture(new RuntimeException()))
            .thenReturn(Futures.<RawMemcacheClient>immediateFuture(delegate));


    ReconnectingClient client = new ReconnectingClient(
            backoffFunction, scheduledExecutorService,
            connector, HostAndPort.fromString("localhost:123"));

    verify(connector, times(3)).connect();
    verify(scheduledExecutorService, times(1)).schedule(Mockito.<Runnable>any(), eq(0L), eq(TimeUnit.MILLISECONDS));
    verify(scheduledExecutorService, times(1)).schedule(Mockito.<Runnable>any(), eq(123L), eq(TimeUnit.MILLISECONDS));
    verify(backoffFunction, times(1)).getBackoffTimeMillis(0);
    verify(backoffFunction, times(1)).getBackoffTimeMillis(1);

    verifyNoMoreInteractions(connector, scheduledExecutorService, backoffFunction);

    assertTrue(client.isConnected());

  }

  @Test
  public void testLostConnectionRetry() throws Exception {
    ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
    when(scheduledExecutorService.schedule(Mockito.<Runnable>any(), anyLong(), Matchers.<TimeUnit>any()))
            .thenAnswer(new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
                runnable.run();
                return null;
              }
            });

    FakeClient delegate1 = new FakeClient(true, true);
    FakeClient delegate2 = new FakeClient(true, false);

    BackoffFunction backoffFunction = mock(BackoffFunction.class);
    when(backoffFunction.getBackoffTimeMillis(0)).thenReturn(0L);
    when(backoffFunction.getBackoffTimeMillis(1)).thenReturn(123L);

    ReconnectingClient.Connector connector = mock(ReconnectingClient.Connector.class);
    when(connector.connect())
            .thenReturn(Futures.<RawMemcacheClient>immediateFuture(delegate1))
            .thenReturn(Futures.<RawMemcacheClient>immediateFailedFuture(new RuntimeException()))
            .thenReturn(Futures.<RawMemcacheClient>immediateFailedFuture(new RuntimeException()))
            .thenReturn(Futures.<RawMemcacheClient>immediateFuture(delegate2));


    ReconnectingClient client = new ReconnectingClient(
            backoffFunction, scheduledExecutorService,
            connector, HostAndPort.fromString("localhost:123"));

    verify(connector, times(4)).connect();
    verify(scheduledExecutorService, times(1)).schedule(Mockito.<Runnable>any(), eq(0L), eq(TimeUnit.MILLISECONDS));
    verify(scheduledExecutorService, times(1)).schedule(Mockito.<Runnable>any(), eq(123L), eq(TimeUnit.MILLISECONDS));
    verify(backoffFunction, times(1)).getBackoffTimeMillis(0);
    verify(backoffFunction, times(1)).getBackoffTimeMillis(1);

    verifyNoMoreInteractions(connector, scheduledExecutorService, backoffFunction);


    assertTrue(client.isConnected());
  }

  private static class FakeClient extends AbstractRawMemcacheClient {

    private boolean connected;
    private boolean disconnectImmediately;

    private FakeClient(boolean connected, boolean disconnectImmediately) {
      this.connected = connected;
      this.disconnectImmediately = disconnectImmediately;
    }

    @Override
    public <T> ListenableFuture<T> send(Request<T> request) {
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
}

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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.BackoffFunction;
import com.spotify.folsom.ConnectFuture;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.DefaultRawMemcacheClient;
import com.spotify.folsom.client.NotConnectedClient;
import com.spotify.folsom.client.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ReconnectingClient extends AbstractRawMemcacheClient {

  private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE =
          Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("folsom-reconnecter")
                  .build());

  private final Logger log = LoggerFactory.getLogger(ReconnectingClient.class);

  private final BackoffFunction backoffFunction;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Connector connector;
  private final HostAndPort address;

  private volatile RawMemcacheClient client = NotConnectedClient.INSTANCE;
  private volatile int reconnectCount = 0;
  private volatile boolean stayConnected = true;

  public ReconnectingClient(final BackoffFunction backoffFunction,
                            final ScheduledExecutorService scheduledExecutorService,
                            final HostAndPort address,
                            final int outstandingRequestLimit,
                            final boolean binary,
                            final Executor executor,
                            final long timeoutMillis,
                            final Charset charset,
                            final Metrics metrics,
                            final int maxSetLength) {
    this(backoffFunction, scheduledExecutorService, new Connector() {
      @Override
      public ListenableFuture<RawMemcacheClient> connect() {
        return DefaultRawMemcacheClient.connect(
                address, outstandingRequestLimit,
                binary, executor, timeoutMillis, charset, metrics, maxSetLength);
      }
    }, address);
  }

  ReconnectingClient(final BackoffFunction backoffFunction,
                     final ScheduledExecutorService scheduledExecutorService,
                     final Connector connector,
                     final HostAndPort address) {
    super();
    this.backoffFunction = backoffFunction;
    this.scheduledExecutorService = scheduledExecutorService;
    this.connector = connector;

    this.address = address;
    retry();
  }

  @Override
  public <T> ListenableFuture<T> send(final Request<T> request) {
    return client.send(request);
  }

  @Override
  public void shutdown() {
    stayConnected = false;
    client.shutdown();
  }

  @Override
  public boolean isConnected() {
    return client.isConnected();
  }

  @Override
  public int numTotalConnections() {
    return client.numTotalConnections();
  }

  @Override
  public int numActiveConnections() {
    return client.numActiveConnections();
  }

  private void retry() {
    try {
      final ListenableFuture<RawMemcacheClient> future = connector.connect();
      Futures.addCallback(future, new FutureCallback<RawMemcacheClient>() {
        @Override
        public void onSuccess(final RawMemcacheClient result) {
          log.info("Successfully connected to {}", address);
          reconnectCount = 0;
          client = result;
          notifyConnectionChange();
          Futures.addCallback(ConnectFuture.disconnectFuture(result), new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void ignore) {
              log.info("Lost connection to {}", address);
              notifyConnectionChange();
              if (stayConnected) {
                retry();
              }
            }

            @Override
            public void onFailure(final Throwable t) {
              throw new RuntimeException("Programmer bug - this should be unreachable");
            }
          });
        }

        @Override
        public void onFailure(final Throwable t) {
          ReconnectingClient.this.onFailure();
        }
      });
    } catch (final Exception e) {
      ReconnectingClient.this.onFailure();
    }
  }

  private void onFailure() {
    final long backOff = backoffFunction.getBackoffTimeMillis(reconnectCount);

    if (stayConnected) {
      log.warn("Attempting reconnect to {} in {} ms (retry number {})",
               address, backOff, reconnectCount);
    }

    scheduledExecutorService.schedule(new Runnable() {
      @Override
      public void run() {
        reconnectCount++;
        if (stayConnected) {
          retry();
        }
      }
    }, backOff, TimeUnit.MILLISECONDS);
  }

  public static ScheduledExecutorService singletonExecutor() {
    return SCHEDULED_EXECUTOR_SERVICE;
  }

  interface Connector {
    ListenableFuture<RawMemcacheClient> connect();
  }

  @Override
  public String toString() {
    return "Reconnecting(" + client + ")";
  }

}

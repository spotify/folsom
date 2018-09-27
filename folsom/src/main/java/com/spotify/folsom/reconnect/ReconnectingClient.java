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

import com.spotify.folsom.MemcacheAuthenticationException;
import com.spotify.folsom.authenticate.AuthenticatingClient;
import com.spotify.folsom.authenticate.Authenticator;
import com.spotify.folsom.authenticate.AsciiAuthenticationValidator;
import com.spotify.folsom.authenticate.BinaryAuthenticationValidator;
import com.spotify.folsom.guava.HostAndPort;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.BackoffFunction;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.DefaultRawMemcacheClient;
import com.spotify.folsom.client.NotConnectedClient;
import com.spotify.folsom.client.Request;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
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
  private final com.spotify.folsom.reconnect.Connector connector;
  private final HostAndPort address;

  private volatile RawMemcacheClient client = NotConnectedClient.INSTANCE;
  private volatile int reconnectCount = 0;
  private volatile boolean stayConnected = true;
  private volatile Throwable connectionFailure;

  public ReconnectingClient(final BackoffFunction backoffFunction,
                            final ScheduledExecutorService scheduledExecutorService,
                            final HostAndPort address,
                            final int outstandingRequestLimit,
                            final boolean binary,
                            final Authenticator authenticator,
                            final Executor executor,
                            final long timeoutMillis,
                            final Charset charset,
                            final Metrics metrics,
                            final int maxSetLength,
                            final EventLoopGroup eventLoopGroup,
                            final Class<? extends Channel> channelClass) {
    this(backoffFunction, scheduledExecutorService, () -> DefaultRawMemcacheClient.connect(
        address, outstandingRequestLimit, binary, executor, timeoutMillis, charset,
        metrics, maxSetLength, eventLoopGroup, channelClass), binary, authenticator, address);
  }

  public ReconnectingClient(final BackoffFunction backoffFunction,
                            final ScheduledExecutorService scheduledExecutorService,
                            final HostAndPort address,
                            final int outstandingRequestLimit,
                            final boolean binary,
                            final Executor executor,
                            final long timeoutMillis,
                            final Charset charset,
                            final Metrics metrics,
                            final int maxSetLength,
                            final EventLoopGroup eventLoopGroup,
                            final Class<? extends Channel> channelClass) {
    this(backoffFunction, scheduledExecutorService, () -> DefaultRawMemcacheClient.connect(
        address, outstandingRequestLimit, binary, executor, timeoutMillis, charset, metrics,
        maxSetLength, eventLoopGroup, channelClass), binary, getValidator(binary), address);
  }

  private static Authenticator getValidator(final boolean binary) {
    if (binary) {
      return new BinaryAuthenticationValidator();
    } else {
      return new AsciiAuthenticationValidator();
    }
  }

  private ReconnectingClient(final BackoffFunction backoffFunction,
                             final ScheduledExecutorService scheduledExecutorService,
                             final Connector connector,
                             final boolean binary,
                             final Authenticator authenticator,
                             final HostAndPort address) {
    this(backoffFunction, scheduledExecutorService,
        () -> AuthenticatingClient.authenticate(connector, binary, authenticator), address);
  }

  ReconnectingClient(final BackoffFunction backoffFunction,
                     final ScheduledExecutorService scheduledExecutorService,
                     final com.spotify.folsom.reconnect.Connector connector,
                     final HostAndPort address) {
    super();
    this.backoffFunction = backoffFunction;
    this.scheduledExecutorService = scheduledExecutorService;
    this.connector = connector;

    this.address = address;
    retry();
  }

  @Override
  public <T> CompletionStage<T> send(final Request<T> request) {
    return client.send(request);
  }

  @Override
  public void shutdown() {
    stayConnected = false;
    client.shutdown();
    notifyConnectionChange();
  }

  @Override
  public boolean isConnected() {
    return client.isConnected();
  }

  @Override
  public Throwable getConnectionFailure() {
    return connectionFailure;
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
      final CompletionStage<RawMemcacheClient> future = connector.connect();
      future.whenComplete((newClient, t) -> {
        if (t != null) {
          if (t instanceof CompletionException) {
            if (t.getCause() instanceof MemcacheAuthenticationException) {
              connectionFailure = t.getCause();
              shutdown();
              return;
            }
          }
          ReconnectingClient.this.onFailure();
        } else {
          log.info("Successfully connected to {}", address);
          reconnectCount = 0;
          client.shutdown();
          client = newClient;

          // Protection against races with shutdown()
          if (!stayConnected) {
            newClient.shutdown();
            notifyConnectionChange();
            return;
          }

          notifyConnectionChange();
          newClient.disconnectFuture().thenRun(() -> {
            log.info("Lost connection to {}", address);
            notifyConnectionChange();
            if (stayConnected) {
              retry();
            }
          });
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

    scheduledExecutorService.schedule(() -> {
      reconnectCount++;
      if (stayConnected) {
        retry();
      }
    }, backOff, TimeUnit.MILLISECONDS);
  }

  public static ScheduledExecutorService singletonExecutor() {
    return SCHEDULED_EXECUTOR_SERVICE;
  }

  @Override
  public String toString() {
    return "Reconnecting(" + client + ")";
  }

}

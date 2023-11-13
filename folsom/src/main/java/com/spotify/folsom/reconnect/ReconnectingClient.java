/*
 * Copyright (c) 2014-2023 Spotify AB
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.BackoffFunction;
import com.spotify.folsom.MemcacheAuthenticationException;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.authenticate.AuthenticatingClient;
import com.spotify.folsom.authenticate.Authenticator;
import com.spotify.folsom.client.DefaultRawMemcacheClient;
import com.spotify.folsom.client.NotConnectedClient;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.folsom.ketama.AddressAndClient;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.Charset;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconnectingClient extends AbstractRawMemcacheClient {

  private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE =
      Executors.newScheduledThreadPool(
          1,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("folsom-reconnecter").build());

  private static final Logger log = LoggerFactory.getLogger(ReconnectingClient.class);

  private final BackoffFunction backoffFunction;
  private final ScheduledExecutorService scheduledExecutorService;
  private final com.spotify.folsom.reconnect.Connector connector;
  private final HostAndPort address;
  private final ReconnectionListener reconnectionListener;

  private volatile RawMemcacheClient client = NotConnectedClient.INSTANCE;
  private volatile int reconnectCount = 0;
  private volatile boolean stayConnected = true;
  private volatile Throwable connectionFailure;

  public ReconnectingClient(
      final BackoffFunction backoffFunction,
      final ScheduledExecutorService scheduledExecutorService,
      final HostAndPort address,
      final ReconnectionListener reconnectionListener,
      final int outstandingRequestLimit,
      final int eventLoopThreadFlushMaxBatchSize,
      final boolean binary,
      final Authenticator authenticator,
      final Executor executor,
      final long connectionTimeoutMillis,
      final Charset charset,
      final Metrics metrics,
      final int maxSetLength,
      final EventLoopGroup eventLoopGroup,
      final Class<? extends Channel> channelClass) {
    this(
        backoffFunction,
        scheduledExecutorService,
        () ->
            DefaultRawMemcacheClient.connect(
                address,
                outstandingRequestLimit,
                eventLoopThreadFlushMaxBatchSize,
                binary,
                executor,
                connectionTimeoutMillis,
                charset,
                metrics,
                maxSetLength,
                eventLoopGroup,
                channelClass),
        authenticator,
        address,
        reconnectionListener);
  }

  public ReconnectingClient(
      final BackoffFunction backoffFunction,
      final ScheduledExecutorService scheduledExecutorService,
      final HostAndPort address,
      final int outstandingRequestLimit,
      final int eventLoopThreadFlushMaxBatchSize,
      final boolean binary,
      final Authenticator authenticator,
      final Executor executor,
      final long connectionTimeoutMillis,
      final Charset charset,
      final Metrics metrics,
      final int maxSetLength,
      final EventLoopGroup eventLoopGroup,
      final Class<? extends Channel> channelClass) {
    this(
        backoffFunction,
        scheduledExecutorService,
        () ->
            DefaultRawMemcacheClient.connect(
                address,
                outstandingRequestLimit,
                eventLoopThreadFlushMaxBatchSize,
                binary,
                executor,
                connectionTimeoutMillis,
                charset,
                metrics,
                maxSetLength,
                eventLoopGroup,
                channelClass),
        authenticator,
        address,
        new StandardReconnectionListener());
  }

  private ReconnectingClient(
      final BackoffFunction backoffFunction,
      final ScheduledExecutorService scheduledExecutorService,
      final Connector connector,
      final Authenticator authenticator,
      final HostAndPort address,
      final ReconnectionListener reconnectionListener) {
    this(
        backoffFunction,
        scheduledExecutorService,
        () -> AuthenticatingClient.authenticate(connector, authenticator),
        address,
        reconnectionListener);
  }

  ReconnectingClient(
      final BackoffFunction backoffFunction,
      final ScheduledExecutorService scheduledExecutorService,
      final com.spotify.folsom.reconnect.Connector connector,
      final HostAndPort address,
      final ReconnectionListener reconnectionListener) {
    super();
    this.backoffFunction = backoffFunction;
    this.scheduledExecutorService = scheduledExecutorService;
    this.connector = connector;
    this.reconnectionListener = reconnectionListener;

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

  @Override
  public int numPendingRequests() {
    return this.client.numPendingRequests();
  }

  @Override
  public Stream<AddressAndClient> streamNodes() {
    return client.streamNodes();
  }

  private void retry() {
    try {
      final CompletionStage<RawMemcacheClient> future = connector.connect();
      future.whenComplete(
          (newClient, t) -> {
            if (t != null) {
              this.reconnectionListener.connectionFailure(t);

              if (t instanceof CompletionException
                  && t.getCause() instanceof MemcacheAuthenticationException) {
                connectionFailure = t.getCause();
                shutdown();
                return;
              }
              ReconnectingClient.this.onFailure(t);
            } else {
              reconnectionListener.reconnectionSuccessful(address, reconnectCount, stayConnected);
              reconnectCount = 0;
              client.shutdown();
              client = newClient;

              // Protection against races with shutdown()
              if (!stayConnected) {
                reconnectionListener.reconnectionCancelled();
                newClient.shutdown();
                notifyConnectionChange();
                return;
              }

              notifyConnectionChange();
              newClient
                  .disconnectFuture()
                  .whenComplete(
                      (unused, throwable) ->
                          reconnectionListener.connectionLost(throwable, address))
                  .thenRun(
                      () -> {
                        notifyConnectionChange();
                        if (stayConnected) {
                          retry();
                        }
                      });
            }
          });
    } catch (final Exception e) {
      ReconnectingClient.this.onFailure(e);
    }
  }

  private void onFailure(final Throwable cause) {
    if (!stayConnected) {
      this.reconnectionListener.reconnectionCancelled();
      return;
    }

    final long backOff = backoffFunction.getBackoffTimeMillis(reconnectCount);

    reconnectCount++;
    this.reconnectionListener.reconnectionQueuedFromError(cause, address, backOff, reconnectCount);

    scheduledExecutorService.schedule(
        () -> {
          if (stayConnected) {
            retry();
          }
        },
        backOff,
        TimeUnit.MILLISECONDS);
  }

  public static ScheduledExecutorService singletonExecutor() {
    return SCHEDULED_EXECUTOR_SERVICE;
  }

  @Override
  public String toString() {
    return "Reconnecting(" + client + ")";
  }

  /**
   * This class should be regarded as <b>internal API</b>. We recognise it may be useful outside
   * internals, to which end it is exposed. All methods are <b>unstable</b> and can change with
   * breakage in patch versions. If you just want your own listener with a stable API, you could
   * extend {@link AbstractReconnectionListener} yourself and delegate calls to this.
   */
  public static class StandardReconnectionListener extends AbstractReconnectionListener {

    public StandardReconnectionListener() {}

    @Override
    public void connectionLost(final @Nullable Throwable cause, final HostAndPort address) {
      log.info("Lost connection to {}", address);
    }

    @Override
    public void reconnectionSuccessful(
        final HostAndPort address, final int attempt, final boolean willStayConnected) {
      log.info("Successfully connected to {}", address);
    }

    @Override
    public void reconnectionQueuedFromError(
        final Throwable cause,
        final HostAndPort address,
        final long backOffMillis,
        final int attempt) {
      log.warn(
          "Attempting reconnect to {} in {} ms (retry number {})", address, backOffMillis, attempt);
    }
  }
}

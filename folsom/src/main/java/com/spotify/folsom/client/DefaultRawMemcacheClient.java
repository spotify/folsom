/*
 * Copyright (c) 2014-2018 Spotify AB
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

import com.google.common.collect.Queues;
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.futures.CompletableFutures;

import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.MemcacheClosedException;
import com.spotify.folsom.MemcacheOverloadedException;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.ascii.AsciiMemcacheDecoder;
import com.spotify.folsom.client.binary.BinaryMemcacheDecoder;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.util.concurrent.DefaultThreadFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DefaultRawMemcacheClient extends AbstractRawMemcacheClient {

  private static final AtomicInteger GLOBAL_CONNECTION_COUNT = new AtomicInteger();

  private final Logger log = LoggerFactory.getLogger(DefaultRawMemcacheClient.class);

  private final AtomicInteger pendingCounter = new AtomicInteger();
  private final int outstandingRequestLimit;

  private final Channel channel;
  private final BatchFlusher flusher;
  private final HostAndPort address;
  private final Executor executor;
  private final long timeoutMillis;
  private final int maxSetLength;

  private final AtomicReference<String> disconnectReason = new AtomicReference<>(null);

  public static CompletionStage<RawMemcacheClient> connect(
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

    final ChannelInboundHandler decoder;
    if (binary) {
      decoder = new BinaryMemcacheDecoder();
    } else {
      decoder = new AsciiMemcacheDecoder(charset);
    }

    final ChannelHandler initializer = new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(final Channel ch) throws Exception {
        ch.pipeline().addLast(
            new TcpTuningHandler(),
            decoder,

            // Downstream
            new MemcacheEncoder()
        );
      }
    };

    final CompletableFuture<RawMemcacheClient> clientFuture = new CompletableFuture<>();

    EventLoopGroup effectiveELG = eventLoopGroup != null ? eventLoopGroup : defaultEventLoopGroup();
    Class<? extends Channel> effectiveChannelClass = channelClass != null ? channelClass :
            defaultChannelClass(effectiveELG);

    final Bootstrap bootstrap = new Bootstrap()
        .group(effectiveELG)
        .handler(initializer)
        .channel(effectiveChannelClass)
        .option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, SimpleSizeEstimator.INSTANCE);

    final ChannelFuture connectFuture = bootstrap.connect(
        new InetSocketAddress(address.getHostText(), address.getPort()));

    connectFuture.addListener((ChannelFutureListener) future -> {
      if (future.isSuccess()) {
        // Create client
        final RawMemcacheClient client = new DefaultRawMemcacheClient(
            address,
            future.channel(),
            outstandingRequestLimit,
            executor,
            timeoutMillis,
            metrics,
            maxSetLength);
        clientFuture.complete(client);
      } else {
        future.channel().close();
        clientFuture.completeExceptionally(future.cause());
      }
    });

    return onExecutor(clientFuture, executor);
  }

  private static EventLoopGroup defaultEventLoopGroup() {
    ThreadFactory factory = new DefaultThreadFactory(DefaultRawMemcacheClient.class, true);
    return Epoll.isAvailable() ? new EpollEventLoopGroup(0, factory) :
            new NioEventLoopGroup(0, factory);
  }

  private static Class<? extends Channel> defaultChannelClass(EventLoopGroup elg) {
    return elg instanceof EpollEventLoopGroup ? EpollSocketChannel.class : NioSocketChannel.class;
  }

  private DefaultRawMemcacheClient(final HostAndPort address,
                                   final Channel channel,
                                   final int outstandingRequestLimit,
                                   final Executor executor, final long timeoutMillis,
                                   final Metrics metrics, int maxSetLength) {
    this.address = address;
    this.executor = executor;
    this.timeoutMillis = timeoutMillis;
    this.maxSetLength = maxSetLength;
    this.channel = checkNotNull(channel, "channel");
    this.flusher = new BatchFlusher(channel);
    this.outstandingRequestLimit = outstandingRequestLimit;

    GLOBAL_CONNECTION_COUNT.incrementAndGet();

    metrics.registerOutstandingRequestsGauge(pendingCounter::get);

    channel.pipeline().addLast("handler", new ConnectionHandler());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> CompletionStage<T> send(final Request<T> request) {
    if (request instanceof SetRequest) {
      SetRequest setRequest = (SetRequest) request;
      byte[] value = setRequest.getValue();
      if (value.length > maxSetLength) {
        return (CompletionStage<T>) onExecutor(
            CompletableFuture.completedFuture(MemcacheStatus.VALUE_TOO_LARGE));
      }
    }

    if (!tryIncrementPending()) {

      // Do the disconnect check in here instead of outside
      // to get better performance in the happy case.
      String disconnectReason = this.disconnectReason.get();
      if (disconnectReason != null) {
        MemcacheClosedException exception = new MemcacheClosedException(disconnectReason);
        return onExecutor(CompletableFutures.exceptionallyCompletedFuture(exception));
      }

      return onExecutor(
          CompletableFutures.exceptionallyCompletedFuture(
              new MemcacheOverloadedException("too many outstanding requests")));
    }
    channel.write(request, new RequestWritePromise(channel, request));
    flusher.flush();
    return onExecutor(request);
  }

  private <T> CompletionStage<T> onExecutor(CompletionStage<T> future) {
    return onExecutor(future, executor);
  }

  private static <T> CompletionStage<T> onExecutor(CompletionStage<T> future, Executor executor) {
    if (executor == null) {
      return future;
    }
    return future.whenCompleteAsync((result, t) -> { }, executor);
  }

  /**
   * Increment the {@link #pendingCounter}, saving a volatile read in the fast path by doing the
   * limit check in the CAS loop.
   *
   * @return true if the limit was not exceeded, false otherwise.
   */
  private boolean tryIncrementPending() {
    int pending;
    do {
      pending = pendingCounter.get();
      if (pending >= outstandingRequestLimit) {
        return false;
      }
    } while (!pendingCounter.compareAndSet(pending, pending + 1));
    return true;
  }

  @Override
  public void shutdown() {
    setDisconnected("Shutdown");
  }

  @Override
  public boolean isConnected() {
    return disconnectReason.get() == null;
  }

  @Override
  public int numTotalConnections() {
    return 1;
  }

  @Override
  public int numActiveConnections() {
    return isConnected() ? 1 : 0;
  }

  /**
   * Handles a channel connected to the address specified in the constructor.
   */
  private class ConnectionHandler extends ChannelDuplexHandler {

    private final Queue<Request<?>> outstanding = Queues.newArrayDeque();
    private final TimeoutChecker<Request<?>> timeoutChecker = TimeoutChecker.create(
        MILLISECONDS, timeoutMillis);

    private final Future<?> timeoutCheckTask;

    ConnectionHandler() {
      final long pollIntervalMillis = Math.min(timeoutMillis, SECONDS.toMillis(1));
      timeoutCheckTask = channel.eventLoop().scheduleWithFixedDelay(() -> {
        final Request<?> head = outstanding.peek();
        if (head == null) {
          return;
        }
        if (timeoutChecker.check(head)) {
          log.error("Request timeout: {} {}", channel, head);
          DefaultRawMemcacheClient.this.setDisconnected("Timeout");
        }
      }, pollIntervalMillis, pollIntervalMillis, MILLISECONDS);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg,
                      final ChannelPromise promise)
        throws Exception {
      Request<?> request = (Request<?>) msg;
      outstanding.add(request);

      super.write(ctx, msg, promise);
    }

    @Override
    public void channelUnregistered(final ChannelHandlerContext ctx) throws Exception {
      timeoutCheckTask.cancel(true);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      DefaultRawMemcacheClient.this.setDisconnected("Disconnected");
      while (true) {
        final Request<?> request = outstanding.poll();
        if (request == null) {
          break;
        }
        request.fail(new MemcacheClosedException(disconnectReason.get()));
      }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      final Request<?> request = outstanding.poll();
      if (request == null) {
        throw new Exception("Unexpected response: " + msg);
      }
      pendingCounter.decrementAndGet();
      try {
        request.handle(msg);
      } catch (final Exception exception) {
        log.error("Corrupt protocol: " + exception.getMessage(), exception);
        DefaultRawMemcacheClient.this.setDisconnected(exception);
        request.fail(new MemcacheClosedException(disconnectReason.get()));
        ctx.channel().close();
      }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
        throws Exception {
      if (cause instanceof DecoderException) {
        DefaultRawMemcacheClient.this.setDisconnected(cause.getCause());
      } else if (!isLostConnection(cause)) {
        // default to the safe option of closing the connection on unhandled exceptions
        // use a ReconnectingClient to keep the connection going
        log.error("Unexpected error, closing connection", cause);
        DefaultRawMemcacheClient.this.setDisconnected(cause);
      }
      ctx.close();
    }
  }

  private boolean isLostConnection(final Throwable t) {
    if (t instanceof IOException) {
      final String message = t.getMessage();
      if (t instanceof ConnectException) {
        return message.startsWith("Connection refused:");
      } else if (message.equals("Broken pipe")) {
        return true;
      } else if (message.equals("Connection reset by peer")) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "DefaultRawMemcacheClient(" + address + ")";
  }

  private class RequestWritePromise extends DefaultChannelPromise {

    private final Request<?> request;

    public RequestWritePromise(final Channel channel, final Request<?> request) {
      super(channel);
      this.request = request;
    }

    @Override
    public ChannelPromise setFailure(final Throwable cause) {
      super.setFailure(cause);
      fail(cause);
      return this;
    }

    @Override
    public boolean tryFailure(final Throwable cause) {
      if (super.tryFailure(cause)) {
        fail(cause);
        return true;
      }
      return false;
    }

    private void fail(Throwable cause) {
      setDisconnected(cause);
      request.fail(new MemcacheClosedException(disconnectReason.get()));
    }
  }

  private void setDisconnected(Throwable cause) {
    String message = cause.getMessage();
    if (message == null) {
      message = cause.getClass().getSimpleName();
    }
    setDisconnected(message);
  }

  private void setDisconnected(String message) {
    if (disconnectReason.compareAndSet(null, message)) {

      // Use the pending counter as a way of marking disconnected for performance reasons
      // Once we are disconnected we will not really decrease this value any more anyway.
      pendingCounter.set(Math.max(Integer.MAX_VALUE / 2, outstandingRequestLimit));
      channel.close();
      GLOBAL_CONNECTION_COUNT.decrementAndGet();
      notifyConnectionChange();
    }
  }

  static int getGlobalConnectionCount() {
    return GLOBAL_CONNECTION_COUNT.get();
  }
}

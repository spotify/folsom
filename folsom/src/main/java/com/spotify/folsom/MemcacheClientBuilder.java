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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.spotify.folsom.client.MemcacheEncoder.MAX_KEY_LEN;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.folsom.authenticate.AsciiAuthenticationValidator;
import com.spotify.folsom.authenticate.Authenticator;
import com.spotify.folsom.authenticate.BinaryAuthenticationValidator;
import com.spotify.folsom.authenticate.MultiAuthenticator;
import com.spotify.folsom.authenticate.NoAuthenticationValidation;
import com.spotify.folsom.authenticate.PlaintextAuthenticator;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.NoopTracer;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.client.binary.DefaultBinaryMemcacheClient;
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.folsom.ketama.AddressAndClient;
import com.spotify.folsom.ketama.KetamaMemcacheClient;
import com.spotify.folsom.ketama.ResolvingKetamaClient;
import com.spotify.folsom.reconnect.ReconnectingClient;
import com.spotify.folsom.retry.RetryingClient;
import com.spotify.folsom.roundrobin.RoundRobinMemcacheClient;
import com.spotify.folsom.transcoder.ByteArrayTranscoder;
import com.spotify.folsom.transcoder.SerializableObjectTranscoder;
import com.spotify.folsom.transcoder.StringTranscoder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class MemcacheClientBuilder<V> {

  private static final int DEFAULT_MAX_SET_LENGTH = 1024 * 1024;

  private static final int DEFAULT_MAX_OUTSTANDING = 1000;
  private static final String DEFAULT_HOSTNAME = "127.0.0.1";
  private static final int DEFAULT_PORT = 11211;

  /** Lazily instantiated singleton default executor. */
  private static final Supplier<Executor> DEFAULT_REPLY_EXECUTOR =
      Suppliers.memoize(
              () ->
                  new ForkJoinPool(
                      Runtime.getRuntime().availableProcessors(),
                      ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                      new UncaughtExceptionHandler(),
                      true))
          ::get;

  /** Lazily instantiated singleton default scheduled executor. */
  private static final Supplier<ScheduledExecutorService> DEFAULT_SCHEDULED_EXECUTOR =
      Suppliers.memoize(
              () ->
                  Executors.newSingleThreadScheduledExecutor(
                      new ThreadFactoryBuilder()
                          .setDaemon(true)
                          .setNameFormat("folsom-default-scheduled-executor")
                          .build()))
          ::get;

  private final List<HostAndPort> addresses = new ArrayList<>();
  private int maxOutstandingRequests = DEFAULT_MAX_OUTSTANDING;
  private int eventLoopThreadFlushMaxBatchSize = Settings.DEFAULT_BATCH_SIZE;
  private final Transcoder<V> valueTranscoder;
  private Metrics metrics = NoopMetrics.INSTANCE;
  private Tracer tracer = NoopTracer.INSTANCE;

  private BackoffFunction backoffFunction = new ExponentialBackoff(10L, 60 * 1000L, 2.5);

  private int connections = 1;
  private boolean retry = true;
  private Supplier<Executor> executor = DEFAULT_REPLY_EXECUTOR;
  private Charset charset = StandardCharsets.UTF_8;

  private Resolver resolver;
  private DnsSrvResolver srvResolver; // deprecated, retained for backwards compatibility
  private String srvRecord; // deprecated, retained for backwards compatibility
  private long resolveRefreshPeriod = 60 * 1000L;
  private long shutdownDelay = 60 * 1000L;

  private long connectionTimeoutMillis = 3000;
  private int maxSetLength = DEFAULT_MAX_SET_LENGTH;
  private int maxKeyLength = MAX_KEY_LEN;
  private EventLoopGroup eventLoopGroup;
  private Class<? extends Channel> channelClass;

  private final List<PlaintextAuthenticator> passwords = new ArrayList<>();
  private boolean skipAuth = false;

  /**
   * Create a client builder for byte array values.
   *
   * @return The builder
   */
  public static MemcacheClientBuilder<byte[]> newByteArrayClient() {
    return new MemcacheClientBuilder<>(ByteArrayTranscoder.INSTANCE);
  }

  /**
   * Create a client builder with a basic string transcoder using the UTF-8 Charset.
   *
   * @return The builder
   */
  public static MemcacheClientBuilder<String> newStringClient() {
    return newStringClient(StandardCharsets.UTF_8);
  }

  /**
   * Create a client builder with a basic string transcoder using the supplied Charset.
   *
   * @param charset the Charset to encode and decode String objects with.
   * @return The builder
   */
  public static MemcacheClientBuilder<String> newStringClient(Charset charset) {
    return new MemcacheClientBuilder<>(new StringTranscoder(charset));
  }

  /**
   * Create a client builder for serializable object values.
   *
   * @return The builder
   */
  public static MemcacheClientBuilder<Serializable> newSerializableObjectClient() {
    return new MemcacheClientBuilder<>(SerializableObjectTranscoder.INSTANCE);
  }

  /**
   * Create a client builder with the provided value transcoder.
   *
   * @param valueTranscoder the transcoder to use to encode/decode values.
   */
  public MemcacheClientBuilder(final Transcoder<V> valueTranscoder) {
    this.valueTranscoder = valueTranscoder;
  }

  /**
   * Define the charset encoding for keys. Note that some charsets may not be compatible with the
   * memcache protocol which requires that keys are a string of 8-bit characters with the exclusion
   * of characters in the range [0x00, 0x20]. This is a problem in charsets such as UTF-16 which
   * uses two bytes and one of them could easily be invalid for memcache purposes.
   *
   * <p>UTF-8 and most single-byte charsets should be fine though.
   *
   * @param charset The charset encoding for keys. The default is UTF-8.
   * @return itself
   */
  public MemcacheClientBuilder<V> withKeyCharset(final Charset charset) {
    this.charset = requireNonNull(charset);
    return this;
  }

  /**
   * Define which memcache server to connect to. This may be called multiple times to connect to
   * multiple hosts. If more than one address is given, Ketama will be used to distribute requests.
   *
   * @param hostname a server, using the default memcached port (11211).
   * @return itself
   */
  public MemcacheClientBuilder<V> withAddress(final String hostname) {
    return withAddress(hostname, DEFAULT_PORT);
  }

  /**
   * Define which memcache server to connect to. This may be called multiple times to connect to
   * multiple hosts. If more than one address is given, Ketama will be used to distribute requests.
   *
   * @param host The server hostname
   * @param port The port where memcached is running
   * @return itself
   */
  public MemcacheClientBuilder<V> withAddress(final String host, final int port) {
    this.addresses.add(HostAndPort.fromParts(host, port));
    return this;
  }

  /**
   * Use a dynamic resolved instead of a fixed set of addresses. This means that the set of nodes
   * can change dynamically over time.
   *
   * @param resolver the resolver to use.
   * @return itself
   */
  public MemcacheClientBuilder<V> withResolver(final Resolver resolver) {
    this.resolver = requireNonNull(resolver);
    return this;
  }

  /** Used for backwards compatibility for srvRecord and srvResolver. */
  private void updateResolver() {
    // if the srvRecord is not provided, do not create a resolver
    if (srvRecord != null) {
      final SrvResolver.Builder builder = SrvResolver.newBuilder(srvRecord);
      if (srvResolver != null) {
        builder.withSrvResolver(srvResolver);
      }
      this.resolver = builder.build();
    } else {
      this.resolver = null;
    }
  }

  /** @deprecated Use {@link #withResolver(Resolver)} with {@link SrvResolver} instead. */
  @Deprecated
  public MemcacheClientBuilder<V> withSRVRecord(final String srvRecord) {
    this.srvRecord = requireNonNull(srvRecord);
    updateResolver();
    return this;
  }

  /**
   * This is only used for the dynamic ketama client. This is the maximum time the resolver should
   * be queried for updates. It can be shorter, depending the getTtl values in the resolve result
   *
   * @param periodMillis time in milliseonds. The default is 60 seconds.
   * @return itself
   */
  public MemcacheClientBuilder<V> withResolveRefreshPeriod(final long periodMillis) {
    this.resolveRefreshPeriod = periodMillis;
    return this;
  }

  /** @deprecated Use {@link #withResolveRefreshPeriod(long)} */
  @Deprecated
  public MemcacheClientBuilder<V> withSRVRefreshPeriod(final long periodMillis) {
    return withResolveRefreshPeriod(periodMillis);
  }

  /**
   * This is only used for the dynamic ketama client. When the resolver results has changed, the old
   * client will be shutdown after this much time has passed, in order to complete pending requests.
   *
   * @param shutdownDelay time in milliseconds. The default is 60 seconds.
   * @return itself
   */
  public MemcacheClientBuilder<V> withResolveShutdownDelay(final long shutdownDelay) {
    this.shutdownDelay = shutdownDelay;
    return this;
  }

  /** @deprecated Use {@link #withResolveShutdownDelay(long)} */
  @Deprecated
  public MemcacheClientBuilder<V> withSRVShutdownDelay(final long shutdownDelay) {
    return withResolveShutdownDelay(shutdownDelay);
  }

  /** @deprecated Use {@link #withResolver(Resolver)} with {@link SrvResolver} instead */
  @Deprecated
  public MemcacheClientBuilder<V> withSrvResolver(final DnsSrvResolver srvResolver) {
    this.srvResolver = requireNonNull(srvResolver, "srvResolver");
    updateResolver();
    return this;
  }

  /**
   * Specify how to collect metrics.
   *
   * @param metrics Default is NoopMetrics - which doesn't collect anything.
   * @return itself
   */
  public MemcacheClientBuilder<V> withMetrics(final Metrics metrics) {
    this.metrics = requireNonNull(metrics);
    return this;
  }

  /**
   * Specify how to collect tracing.
   *
   * @param tracer Default is NoopTracer - which doesn't collect anything.
   * @return itself
   */
  public MemcacheClientBuilder<V> withTracer(final Tracer tracer) {
    this.tracer = requireNonNull(tracer);
    return this;
  }

  /**
   * Specify the maximum number of requests in the queue per server connection. If this is set too
   * low, requests will fail with {@link com.spotify.folsom.MemcacheOverloadedException}. If this is
   * set too high, there is a risk of having high latency requests and delaying the time to notice
   * that the system is malfunctioning.
   *
   * @param maxOutstandingRequests the maximum number of requests that can be in queue. Default is
   *     1000.
   * @return itself
   */
  public MemcacheClientBuilder<V> withMaxOutstandingRequests(final int maxOutstandingRequests) {
    this.maxOutstandingRequests = maxOutstandingRequests;
    return this;
  }

  /**
   * Specify the maximum number of requests that will be written to a connection in a single
   * operation when sending requests on the {@link EventLoopGroup} thread that is handling the
   * connection.
   *
   * <p>Note: The name of this configuration property is misleading.
   *
   * <p>Configuring this is only useful in specific circumstances when also using {@link
   * #withEventLoopGroup(EventLoopGroup)} to configure an IO thread pool that is also used to send
   * requests. E.g. when reusing the same {@link EventLoopGroup} for handling incoming network IO
   * and sending memcache requests in a service.
   *
   * @see #withEventLoopThreadFlushMaxBatchSize(int)
   * @param eventLoopThreadFlushMaxBatchSize the maximum number of requests that will be written to
   *     a connection in a single operation when sending requests on the {@link EventLoopGroup}
   *     thread that is handling the connection. Default is {@value Settings#DEFAULT_BATCH_SIZE}.
   * @return itself
   * @deprecated Most users should prefer {@link #withMaxOutstandingRequests(int)}. Some users that
   *     also configure {@link #withEventLoopGroup(EventLoopGroup)} might want to configure {@link
   *     #withEventLoopThreadFlushMaxBatchSize(int)}.
   */
  @Deprecated
  public MemcacheClientBuilder<V> withRequestBatchSize(final int eventLoopThreadFlushMaxBatchSize) {
    return withEventLoopThreadFlushMaxBatchSize(eventLoopThreadFlushMaxBatchSize);
  }

  /**
   * Specify the maximum number of requests that will be written to a connection in a single
   * operation when sending requests on the {@link EventLoopGroup} thread that is handling the
   * connection.
   *
   * <p>Configuring this can be useful in specific circumstances when also using {@link
   * #withEventLoopGroup(EventLoopGroup)} to configure an IO thread pool that is also used to send
   * requests. E.g. when reusing the same {@link EventLoopGroup} for handling incoming network IO
   * and sending memcache requests in a service.
   *
   * <p>If the client's batch-size is larger than your memcached server's value, you may experience
   * an increase in `conn_yields` on your memcached server's stats...which indicates your server is
   * switching to other I/O connections during the batch request to not starve other connections.
   *
   * <p>If this value is too low, your will make more network requests per operation, thus reducing
   * your server's overall throughput.
   *
   * <p>The optimal value should be matched to your workload and roughly the same value as your
   * memcached server's `-R` argument, which defaults to 20.
   *
   * @param eventLoopThreadFlushMaxBatchSize the maximum number of requests that will be written to
   *     a connection in a single operation when sending requests on the {@link EventLoopGroup}
   *     thread that is handling the connection. Default is {@value Settings#DEFAULT_BATCH_SIZE}.
   * @return itself
   */
  public MemcacheClientBuilder<V> withEventLoopThreadFlushMaxBatchSize(
      final int eventLoopThreadFlushMaxBatchSize) {
    checkArgument(eventLoopThreadFlushMaxBatchSize > 0, "batch size must be > 0");
    this.eventLoopThreadFlushMaxBatchSize = eventLoopThreadFlushMaxBatchSize;
    return this;
  }

  /**
   * Specify how long the client should wait between reconnects.
   *
   * @param backoffFunction A custom backoff function. Default is exponential backoff.
   * @return itself.
   */
  public MemcacheClientBuilder<V> withBackoff(final BackoffFunction backoffFunction) {
    this.backoffFunction = backoffFunction;
    return this;
  }

  /**
   * Specify if the client should retry once if the connection is closed. This typically only has an
   * effect when one of the ketama nodes disconnect while the request is sent.
   *
   * @param retry Default is true
   * @return itself
   */
  public MemcacheClientBuilder<V> withRetry(final boolean retry) {
    this.retry = retry;
    return this;
  }

  /**
   * Specify an executor to execute all replies on. Default is a shared {@link ForkJoinPool} in
   * async mode with one thread per processor.
   *
   * <p>If null is specified, replies will be executed on EventLoopGroup directly.
   *
   * <p><b>Note:</b> Calling non-async methods on the {@link java.util.concurrent.CompletionStage}s
   * returned by {@link MemcacheClient} that have already completed will cause the supplied function
   * to be executed directly on the calling thread.
   *
   * @param executor the executor to use.
   * @return itself
   */
  public MemcacheClientBuilder<V> withReplyExecutor(final Executor executor) {
    this.executor = Suppliers.ofInstance(executor)::get;
    return this;
  }

  /**
   * Use multiple connections to each memcache server. This is likely not a useful thing in
   * practice, unless the IO connection really becomes a bottleneck.
   *
   * @param connections Number of connections, must be 1 or greater. The default is 1
   * @return itself
   */
  public MemcacheClientBuilder<V> withConnections(final int connections) {
    if (connections < 1) {
      throw new IllegalArgumentException("connections must be at least 1");
    }
    this.connections = connections;
    return this;
  }

  /**
   * This has been deprecated - see {#link withConnectionTimeoutMillis}.
   *
   * <p>Do not use this to enforce request timeouts. Instead, set a timeout on the request futures
   * using orTimeout() or some other manual mechanism.
   *
   * @param timeoutMillis The timeout in milliseconds. The default is 3000 ms.
   * @return itself
   */
  @Deprecated
  public MemcacheClientBuilder<V> withRequestTimeoutMillis(final long timeoutMillis) {
    return withConnectionTimeoutMillis(timeoutMillis);
  }

  /**
   * Set the maximum time to wait before considering the connection to be dead and should be closed
   * and recreated.
   *
   * <p>Do not use this to enforce request timeouts. Instead, set a timeout on the request futures
   * using orTimeout() or some other manual mechanism.
   *
   * @param timeoutMillis The timeout in milliseconds. The default is 3000 ms.
   * @return itself
   */
  public MemcacheClientBuilder<V> withConnectionTimeoutMillis(final long timeoutMillis) {
    this.connectionTimeoutMillis = timeoutMillis;
    return this;
  }

  /**
   * Set the maximum value size for set requests. If the limit is exceeded the set operation will
   * fast-fail with a VALUE_TOO_LARGE status.
   *
   * <p>If this limit is set higher than the actual limit in the memcache service, the memcache
   * service may return a SERVER_ERROR which will close the connection to prevent any corrupted
   * state.
   *
   * <p>The default value is 1 MiB
   *
   * @param maxSetLength The maximum size in bytes
   * @return itself
   */
  public MemcacheClientBuilder<V> withMaxSetLength(final int maxSetLength) {
    this.maxSetLength = maxSetLength;
    return this;
  }

  /**
   * Set an Netty {@link EventLoopGroup} for the client.
   *
   * <p>If not specified, an event loop group will be created, with default thread count.
   *
   * @param eventLoopGroup an event loop group instance
   * @return itself
   */
  public MemcacheClientBuilder<V> withEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    this.eventLoopGroup = eventLoopGroup;
    return this;
  }

  /**
   * Set a Netty {@link Channel} class for the client.
   *
   * <p>If not specified, it'll use a Channel class, according to the EventLoopGroup in use.
   *
   * @param channelClass a class of channel to use.
   * @return itself
   */
  public MemcacheClientBuilder<V> withChannelClass(final Class<? extends Channel> channelClass) {
    this.channelClass = channelClass;
    return this;
  }

  /**
   * Set the maximum key length of the byte representation of the input string. The default value is
   * 250, and the valid range is [0, 250]
   *
   * @param maxKeyLength The maximum key length in bytes
   * @return itself
   */
  public MemcacheClientBuilder<V> withMaxKeyLength(final int maxKeyLength) {
    if (maxKeyLength < 0) {
      throw new IllegalArgumentException("maxKeyLength must be non-negative");
    }
    if (maxKeyLength > MAX_KEY_LEN) {
      throw new IllegalArgumentException(
          "maxKeyLength must be smaller than " + MAX_KEY_LEN + " but was " + maxKeyLength);
    }
    this.maxKeyLength = maxKeyLength;
    return this;
  }

  /**
   * Authenticate with memcached using plaintext SASL. This only works for binary connections, not
   * ascii.
   *
   * <p>You may call this multiple times, to set multiple authentication attempts. This is typically
   * only useful during a password rotation to avoid downtime.
   *
   * @param username
   * @param password
   * @return itself
   */
  public MemcacheClientBuilder<V> withUsernamePassword(
      final String username, final String password) {
    passwords.add(new PlaintextAuthenticator(username, password));
    return this;
  }

  /**
   * Disable authentication validation - only useful for tests against jmemcached which does not
   * support binary NOOP
   *
   * @return itself
   */
  MemcacheClientBuilder<V> withoutAuthenticationValidation() {
    skipAuth = true;
    return this;
  }

  /**
   * Create a client that uses the binary memcache protocol.
   *
   * @return a {@link com.spotify.folsom.BinaryMemcacheClient}
   */
  public BinaryMemcacheClient<V> connectBinary() {
    final Authenticator authenticator =
        getAuthenticator(BinaryAuthenticationValidator.getInstance());
    authenticator.validate(true);
    return new DefaultBinaryMemcacheClient<>(
        connectRaw(true, authenticator), metrics, tracer, valueTranscoder, charset, maxKeyLength);
  }

  private Authenticator getAuthenticator(Authenticator defaultValue) {
    if (skipAuth) {
      if (!passwords.isEmpty()) {
        throw new IllegalStateException(
            "You may not specify both withoutAuthenticationValidation() and withUsernamePassword()");
      }
      return NoAuthenticationValidation.getInstance();
    }
    if (passwords.isEmpty()) {
      return defaultValue;
    }
    return new MultiAuthenticator(passwords);
  }

  /**
   * Create a client that uses the ascii memcache protocol.
   *
   * @return a {@link com.spotify.folsom.AsciiMemcacheClient}
   */
  public AsciiMemcacheClient<V> connectAscii() {
    final Authenticator authenticator =
        getAuthenticator(AsciiAuthenticationValidator.getInstance());
    authenticator.validate(false);
    return new DefaultAsciiMemcacheClient<>(
        connectRaw(false, authenticator), metrics, tracer, valueTranscoder, charset, maxKeyLength);
  }

  /**
   * Connect a raw memcached client without any protocol specific methods. This should rarely be
   * needed.
   *
   * @param binary whether to use the binary protocol or not.
   * @return A raw memcached client.
   */
  protected RawMemcacheClient connectRaw(boolean binary, Authenticator authenticator) {
    List<HostAndPort> addresses = this.addresses;
    RawMemcacheClient client;
    if (resolver != null) {
      if (!addresses.isEmpty()) {
        throw new IllegalStateException("You may not specify both a resolver and addresses");
      }
      client = createResolvingClient(binary, authenticator);
    } else {
      if (addresses.isEmpty()) {
        addresses = ImmutableList.of(HostAndPort.fromParts(DEFAULT_HOSTNAME, DEFAULT_PORT));
      }

      final List<RawMemcacheClient> clients = createClients(addresses, binary, authenticator);
      if (addresses.size() > 1) {
        checkState(clients.size() == addresses.size());

        final List<AddressAndClient> aac = new ArrayList<>(clients.size());
        for (int i = 0; i < clients.size(); i++) {
          final HostAndPort address = addresses.get(i);
          aac.add(new AddressAndClient(address, clients.get(i)));
        }

        client = new KetamaMemcacheClient(aac);
      } else {
        client = clients.get(0);
      }
    }

    if (retry) {
      return new RetryingClient(client);
    }
    return client;
  }

  private List<RawMemcacheClient> createClients(
      final List<HostAndPort> addresses, final boolean binary, final Authenticator authenticator) {

    final List<RawMemcacheClient> clients = new ArrayList<>(addresses.size());
    for (final HostAndPort address : addresses) {
      clients.add(createClient(address, binary, authenticator));
    }
    return clients;
  }

  private RawMemcacheClient createResolvingClient(
      final boolean binary, final Authenticator authenticator) {
    ResolvingKetamaClient client =
        new ResolvingKetamaClient(
            resolver,
            DEFAULT_SCHEDULED_EXECUTOR.get(),
            resolveRefreshPeriod,
            TimeUnit.MILLISECONDS,
            input -> createClient(input, binary, authenticator),
            shutdownDelay,
            TimeUnit.MILLISECONDS);

    client.start();
    return client;
  }

  private RawMemcacheClient createClient(
      final HostAndPort address, final boolean binary, final Authenticator authenticator) {
    if (connections == 1) {
      return createReconnectingClient(address, binary, authenticator);
    }
    final List<RawMemcacheClient> clients = new ArrayList<>();
    for (int i = 0; i < connections; i++) {
      clients.add(createReconnectingClient(address, binary, authenticator));
    }
    return new RoundRobinMemcacheClient(clients);
  }

  private RawMemcacheClient createReconnectingClient(
      final HostAndPort address, final boolean binary, final Authenticator authenticator) {
    return new ReconnectingClient(
        backoffFunction,
        ReconnectingClient.singletonExecutor(),
        address,
        maxOutstandingRequests,
        eventLoopThreadFlushMaxBatchSize,
        binary,
        authenticator,
        executor.get(),
        connectionTimeoutMillis,
        charset,
        metrics,
        maxSetLength,
        eventLoopGroup,
        channelClass);
  }
}

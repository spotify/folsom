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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.spotify.folsom.client.MemcacheEncoder.MAX_KEY_LEN;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.folsom.authenticate.Authenticator;
import com.spotify.folsom.authenticate.AsciiAuthenticationValidator;
import com.spotify.folsom.authenticate.BinaryAuthenticationValidator;
import com.spotify.folsom.guava.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.DnsSrvResolvers;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.ascii.DefaultAsciiMemcacheClient;
import com.spotify.folsom.client.binary.DefaultBinaryMemcacheClient;
import com.spotify.folsom.ketama.AddressAndClient;
import com.spotify.folsom.ketama.KetamaMemcacheClient;
import com.spotify.folsom.ketama.SrvKetamaClient;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class MemcacheClientBuilder<V> {

  private static final int DEFAULT_MAX_SET_LENGTH = 1024 * 1024;

  private static final int DEFAULT_MAX_OUTSTANDING = 1000;
  private static final String DEFAULT_HOSTNAME = "127.0.0.1";
  private static final int DEFAULT_PORT = 11211;


  /**
   * Lazily instantiated singleton default executor.
   */
  private static class DefaultExecutor {

    private static final Executor INSTANCE = new ForkJoinPool(
            Runtime.getRuntime().availableProcessors(),
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            new UncaughtExceptionHandler(), true);
  }

  /**
   * Lazily instantiated singleton default srvResolver.
   */
  private static class DefaultDnsResolver {
    private static final DnsSrvResolver INSTANCE = DnsSrvResolvers.newBuilder()
            .cachingLookups(true)
            .retainingDataOnFailures(true)
            .build();
  }

  /**
   * Lazily instantiated singleton default scheduled executor.
   */
  private static class DefaultScheduledExecutor {
    private static final ScheduledExecutorService INSTANCE =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("folsom-default-scheduled-executor")
                            .build());
  }

  private final List<HostAndPort> addresses = new ArrayList<>();
  private int maxOutstandingRequests = DEFAULT_MAX_OUTSTANDING;
  private final Transcoder<V> valueTranscoder;
  private Metrics metrics = NoopMetrics.INSTANCE;

  private BackoffFunction backoffFunction =
      new ExponentialBackoff(10L, 60 * 1000L, 2.5);

  private int connections = 1;
  private boolean retry = true;
  private Executor executor = DefaultExecutor.INSTANCE;
  private Charset charset = Charsets.UTF_8;

  private DnsSrvResolver srvResolver;
  private String srvRecord;
  private long dnsRefreshPeriod = 60 * 1000L;
  private long shutdownDelay = 60 * 1000L;

  private long timeoutMillis = 3000;
  private int maxSetLength = DEFAULT_MAX_SET_LENGTH;
  private int maxKeyLength = MAX_KEY_LEN;
  private EventLoopGroup eventLoopGroup;
  private Class<? extends Channel> channelClass;

  /**
   * Create a client builder for byte array values.
   * @return The builder
   */
  public static MemcacheClientBuilder<byte[]> newByteArrayClient() {
    return new MemcacheClientBuilder<>(ByteArrayTranscoder.INSTANCE);
  }

  /**
   * Create a client builder with a basic string transcoder using the UTF-8
   * Charset.
   *
   * @return The builder
   */
  public static MemcacheClientBuilder<String> newStringClient() {
    return newStringClient(Charsets.UTF_8);
  }

  /**
   * Create a client builder with a basic string transcoder using the supplied
   * Charset.
   *
   * @param charset the Charset to encode and decode String objects with.
   * @return The builder
   */
  public static MemcacheClientBuilder<String> newStringClient(Charset charset) {
    return new MemcacheClientBuilder<>(new StringTranscoder(charset));
  }

  /**
   * Create a client builder for serializable object values.
   * @return The builder
   */
  public static MemcacheClientBuilder<Serializable> newSerializableObjectClient() {
    return new MemcacheClientBuilder<>(SerializableObjectTranscoder.INSTANCE);
  }


  /**
   * Create a client builder with the provided value transcoder.
   * @param valueTranscoder the transcoder to use to encode/decode values.
   */
  public MemcacheClientBuilder(final Transcoder<V> valueTranscoder) {
    this.valueTranscoder = valueTranscoder;
  }

  /**
   * Define the charset encoding for keys.
   * Note that some charsets may not be compatible with the memcache protocol which requires
   * that keys are a string of 8-bit characters with the exclusion of characters in the range
   * [0x00, 0x20].
   * This is a problem in charsets such as UTF-16 which uses two bytes and one of them
   * could easily be invalid for memcache purposes.
   *
   * UTF-8 and most single-byte charsets should be fine though.
   *
   * @param charset The charset encoding for keys. The default is UTF-8.
   * @return itself
  */
  public MemcacheClientBuilder<V> withKeyCharset(final Charset charset) {
    this.charset = checkNotNull(charset);
    return this;
  }

  /**
   * Define which memcache server to connect to.
   * This may be called multiple times to connect to multiple hosts.
   * If more than one address is given, Ketama will be used to distribute requests.
   * @param hostname a server, using the default memcached port (11211).
   * @return itself
   */
  public MemcacheClientBuilder<V> withAddress(final String hostname) {
    return withAddress(hostname, DEFAULT_PORT);
  }

  /**
   * Define which memcache server to connect to.
   * This may be called multiple times to connect to multiple hosts.
   * If more than one address is given, Ketama will be used to distribute requests.
   * @param host The server hostname
   * @param port The port where memcached is running
   * @return itself
   */
  public MemcacheClientBuilder<V> withAddress(final String host, final int port) {
    this.addresses.add(HostAndPort.fromParts(host, port));
    return this;
  }

  /**
   * Use SRV to lookup nodes instead of a fixed set of addresses.
   * This means that the set of nodes can change dynamically over time.
   * @param srvRecord the SRV record to use.
   * @return itself
   */
  public MemcacheClientBuilder<V> withSRVRecord(final String srvRecord) {
    this.srvRecord = checkNotNull(srvRecord);
    return this;
  }

  /**
   * This is only used for the SRV based ketama client. This is the maximum
   * time DNS should be queried for updates.
   * It can be shorter, depending the ttl values in the DNS lookup result
   *
   * @param periodMillis time in milliseonds. The default is 60 seconds.
   * @return itself
   */
  public MemcacheClientBuilder<V> withSRVRefreshPeriod(final long periodMillis) {
    this.dnsRefreshPeriod = periodMillis;
    return this;
  }

  /**
   * This is only used for the SRV based ketama client. When the SRV record has changed,
   * the old client will be shutdown after this much time has passed, in order to complete
   * pending requests.
   * @param shutdownDelay time in milliseconds. The default is 60 seconds.
   * @return itself
   */
  public MemcacheClientBuilder<V> withSRVShutdownDelay(final long shutdownDelay) {
    this.shutdownDelay = shutdownDelay;
    return this;
  }

  /**
   * Use a specific SRV resolver.
   * @param srvResolver the resolver to use. Default is a caching resolver from
   *                    {@link com.spotify.dns.DnsSrvResolvers}
   * @return itself
   */
  public MemcacheClientBuilder<V> withSrvResolver(final DnsSrvResolver srvResolver) {
    this.srvResolver = checkNotNull(srvResolver, "srvResolver");
    return this;
  }

  /**
   * Specify how to collect metrics.
   * @param metrics Default is NoopMetrics - which doesn't collect anything.
   * @return itself
   */
  public MemcacheClientBuilder<V> withMetrics(final Metrics metrics) {
    this.metrics = metrics;
    return this;
  }

  /**
   * Specify the maximum number of requests in the queue per server connection.
   * If this is set too low, requests will fail with
   * {@link com.spotify.folsom.MemcacheOverloadedException}.
   * If this is set too high, there is a risk of having high latency requests and delaying the
   * time to notice that the system is malfunctioning.
   * @param maxOutstandingRequests the maximum number of requests that can be in queue.
   *                               Default is 1000.
   * @return itself
   */
  public MemcacheClientBuilder<V> withMaxOutstandingRequests(final int maxOutstandingRequests) {
    this.maxOutstandingRequests = maxOutstandingRequests;
    return this;
  }

  /**
   * Specify how long the client should wait between reconnects.
   * @param backoffFunction A custom backoff function. Default is exponential backoff.
   * @return itself.
   */
  public MemcacheClientBuilder<V> withBackoff(final BackoffFunction backoffFunction) {
    this.backoffFunction = backoffFunction;
    return this;
  }

  /**
   * Specify if the client should retry once if the connection is closed.
   * This typically only has an effect when one of the ketama nodes disconnect while the
   * request is sent.
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
   * If null is specified, replies will be executed on EventLoopGroup directly.
   *
   * <b>Note:</b> Calling non-async methods on the {@link java.util.concurrent.CompletionStage}s
   * returned by {@link MemcacheClient} that have already completed will cause the supplied
   * function to be executed directly on the calling thread.
   *
   * @param executor the executor to use.
   * @return itself
   */
  public MemcacheClientBuilder<V> withReplyExecutor(final Executor executor) {
    this.executor = executor;
    return this;
  }

  /**
   * Use multiple connections to each memcache server.
   * This is likely not a useful thing in practice,
   * unless the IO connection really becomes a bottleneck.
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
   * Enforce a timeout for requests to complete, closing the connection and reconnecting if the
   * timeout is exceeded.
   * @param timeoutMillis The timeout in milliseconds. The default is 3000 ms.
   * @return itself
   */
  public MemcacheClientBuilder<V> withRequestTimeoutMillis(final long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
    return this;
  }

  /**
   * Set the maximum value size for set requests.
   * If the limit is exceeded the set operation will fast-fail with a VALUE_TOO_LARGE status.
   *
   * If this limit is set higher than the actual limit in the memcache service,
   * the memcache service may return a SERVER_ERROR
   * which will close the connection to prevent any corrupted state.
   *
   * The default value is 1 MiB
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
   * If not specified, an event loop group will be created, with default thread count.
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
   * If not specified, it'll use a Channel class, according to the EventLoopGroup in use.
   * @param channelClass a class of channel to use.
   * @return itself
   */
  public MemcacheClientBuilder<V> withChannelClass(final Class<? extends Channel> channelClass) {
    this.channelClass = channelClass;
    return this;
  }

  /**
   * Set the maximum key length of the byte representation of the input string.
   * The default value is 250, and the valid range is [0, 250]
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

  public BinaryMemcacheClient<V> connectBinary(final Authenticator authenticator) {
    return new DefaultBinaryMemcacheClient<>(
        connectRaw(true, authenticator), metrics, valueTranscoder, charset, maxKeyLength);
  }

  /**
   * Create a client that uses the binary memcache protocol.
   * @return a {@link com.spotify.folsom.BinaryMemcacheClient}
   */
  public BinaryMemcacheClient<V> connectBinary() {
    final Authenticator authenticator = new BinaryAuthenticationValidator();
    return new DefaultBinaryMemcacheClient<>(
        connectRaw(true, authenticator), metrics, valueTranscoder, charset, maxKeyLength);
  }

  /**
   * Create a client that uses the ascii memcache protocol.
   * @return a {@link com.spotify.folsom.AsciiMemcacheClient}
   */
  public AsciiMemcacheClient<V> connectAscii() {
    final Authenticator authenticator = new AsciiAuthenticationValidator();
    return new DefaultAsciiMemcacheClient<>(
        connectRaw(false, authenticator), metrics, valueTranscoder, charset, maxKeyLength);
  }

  /**
   * Connect a raw memcached client without any protocol specific methods.
   * This should rarely be needed.
   * @param binary whether to use the binary protocol or not.
   * @return A raw memcached client.
   */
  protected RawMemcacheClient connectRaw(boolean binary, Authenticator authenticator) {
    List<HostAndPort> addresses = this.addresses;
    RawMemcacheClient client;
    if (srvRecord != null) {
      if (!addresses.isEmpty()) {
        throw new IllegalStateException("You may not specify both srvRecord and addresses");
      }
      client = createSRVClient(binary, authenticator);
    } else {
      if (addresses.isEmpty()) {
        addresses = ImmutableList.of(
                HostAndPort.fromParts(DEFAULT_HOSTNAME, DEFAULT_PORT));
      }

      final List<RawMemcacheClient> clients = createClients(addresses, binary, authenticator);
      if (addresses.size() > 1) {
        checkState(clients.size() == addresses.size());

        final List<AddressAndClient> aac = Lists.newArrayListWithCapacity(clients.size());
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

  private List<RawMemcacheClient> createClients(final List<HostAndPort> addresses,
                                                final boolean binary,
                                                final Authenticator authenticator) {

    final List<RawMemcacheClient> clients = Lists.newArrayListWithCapacity(addresses.size());
    for (final HostAndPort address : addresses) {
      clients.add(createClient(address, binary, authenticator));
    }
    return clients;
  }

  private RawMemcacheClient createSRVClient(final boolean binary,
                                            final Authenticator authenticator) {
    DnsSrvResolver resolver = srvResolver;
    if (resolver == null) {
      resolver = DefaultDnsResolver.INSTANCE;
    }

    SrvKetamaClient client = new SrvKetamaClient(srvRecord, resolver,
            DefaultScheduledExecutor.INSTANCE,
            dnsRefreshPeriod, TimeUnit.MILLISECONDS,
            input -> createClient(input, binary, authenticator),
            shutdownDelay, TimeUnit.MILLISECONDS);

    client.start();
    return client;
  }

  private RawMemcacheClient createClient(final HostAndPort address,
                                         final boolean binary,
                                         final Authenticator authenticator) {
    if (connections == 1) {
      return createReconnectingClient(address, binary, authenticator);
    }
    final List<RawMemcacheClient> clients = Lists.newArrayList();
    for (int i = 0; i < connections; i++) {
      clients.add(createReconnectingClient(address, binary, authenticator));
    }
    return new RoundRobinMemcacheClient(clients);
  }

  private  RawMemcacheClient createReconnectingClient(final HostAndPort address,
                                                      final boolean binary,
                                                      final Authenticator authenticator) {
    return new ReconnectingClient(
        backoffFunction,
        ReconnectingClient.singletonExecutor(),
        address,
        maxOutstandingRequests,
        binary,
        authenticator,
        executor,
        timeoutMillis,
        charset,
        metrics,
        maxSetLength,
        eventLoopGroup,
        channelClass);
  }
}

/*
 * Copyright (c) 2014-2019 Spotify AB
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

package com.spotify.folsom.elasticache;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

import com.spotify.folsom.Resolver;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

/**
 * Implement support for AWS ElastiCache node auto-discovery <a
 * href="https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.AddingToYourClientLibrary.html">as
 * documented</a>.
 *
 * <p>Example use:
 *
 * <pre>{@code
 * ElastiCacheResolver resolver = ElastiCacheResolver
 *          .newBuilder("cluster-configuration-endpoint-hostname")
 *          .build();
 *
 *  MemcacheClientBuilder.newStringClient()
 *          .withResolver(resolver)
 *          ...
 * }</pre>
 */
public class ElastiCacheResolver implements Resolver {

  public static class Builder {
    private final String configHost;
    private int configPort = 11211;
    private long ttl = MINUTES.toMillis(1);
    private int timeout = 5000; // ms

    private Builder(final String configHost) {
      this.configHost = configHost;
    }

    /**
     * Set the configuration endpoint port. Default: 11211.
     *
     * @param port The port
     * @return The builder
     */
    public Builder withConfigPort(final int port) {
      checkArgument(port > 0, "port must be an integer 1-65535");
      checkArgument(port < 65536, "port must be an integer 1-65535");
      this.configPort = port;
      return this;
    }

    /**
     * Set the time to live for a resolution result. That is, this controls how frequent the list of
     * cluster nodes is renewed. Default: 600000 ms (1 min)
     *
     * @param ttl Time to live in milliseconds
     * @return The builder
     */
    public Builder withTtlMillis(final long ttl) {
      checkArgument(ttl > 0, "ttl must be a positive integer");
      this.ttl = ttl;
      return this;
    }

    /**
     * Set the socket timeout for a resolution attempt. Default: 5000 ms
     *
     * @param timeout The timeout in milliseconds
     * @return The builder
     */
    public Builder withResolveTimeoutMillis(final int timeout) {
      checkArgument(timeout > 0, "timeout must be a positive integer");
      this.timeout = timeout;
      return this;
    }

    /**
     * Build the resolver
     *
     * @return The resolver
     */
    public ElastiCacheResolver build() {
      return new ElastiCacheResolver(configHost, configPort, ttl, timeout);
    }
  }

  private static final byte[] CMD = "config get cluster\n".getBytes(US_ASCII);

  /**
   * Build a new resolver.
   *
   * @param configHost The configuration endpoint hostname, e.g
   *     foo.o18xjv.cfg.euw1.cache.amazonaws.com
   * @return The builder
   */
  public static Builder newBuilder(final String configHost) {
    return new Builder(configHost);
  }

  private final String configHost;
  private final int configPort;
  private final long ttl;
  private final int timeout;
  private final ResponseParser parser;

  private ElastiCacheResolver(
      final String configHost, final int configPort, final long ttl, final int timeout) {
    this.configHost = configHost;
    this.configPort = configPort;
    this.ttl = ttl;
    this.parser = new ResponseParser();
    this.timeout = timeout;
  }

  @Override
  public List<ResolveResult> resolve() {
    try (final Socket socket = new Socket()) {
      socket.setSoTimeout(timeout);
      socket.connect(new InetSocketAddress(configHost, configPort), timeout);

      socket.getOutputStream().write(CMD);

      return parser
          .parse(socket.getInputStream())
          .stream()
          .map(hap -> new ResolveResult(hap.getHostText(), hap.getPort(), ttl))
          .collect(toList());
    } catch (IOException e) {
      throw new RuntimeException("ElastiCache auto-discovery failed", e);
    }
  }
}

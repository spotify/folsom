/*
 * Copyright (c) 2018 Spotify AB
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

import com.google.common.base.Suppliers;
import com.spotify.folsom.client.tls.DefaultSSLEngineFactory;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;

public class MemcachedServer {

  public enum AuthenticationMode {
    NONE,
    SASL,
    ASCII
  }

  private final FixedHostPortGenericContainer container;
  private MemcacheClient<String> client;

  public static final Supplier<MemcachedServer> SIMPLE_INSTANCE =
      Suppliers.memoize(MemcachedServer::new)::get;

  public static int DEFAULT_PORT = 11211;

  private static final String MEMCACHED_VERSION = "1.6.21";
  private final String username;
  private final String password;
  private final boolean useTLS;

  public MemcachedServer() {
    this(null, null);
  }

  public MemcachedServer(String username, String password) {
    this(username, password, DEFAULT_PORT, AuthenticationMode.NONE, false);
  }

  public MemcachedServer(String username, String password, AuthenticationMode authenticationMode) {
    this(username, password, DEFAULT_PORT, authenticationMode, false);
  }

  public MemcachedServer(boolean useTLS) {
    this(null, null, DEFAULT_PORT, AuthenticationMode.NONE, useTLS);
  }

  public MemcachedServer(
      String username,
      String password,
      int port,
      AuthenticationMode authenticationMode,
      boolean useTLS) {
    this.username = username;
    this.password = password;
    this.useTLS = useTLS;

    if (useTLS) {
      if (authenticationMode != AuthenticationMode.NONE) {
        throw new RuntimeException(
            "Authentication is not currently supported with the TLS-enabled container");
      }

      container = setupTLSContainer();
    } else {
      container = setupContainer(username, password, authenticationMode);
    }

    if (port != DEFAULT_PORT) {
      container.withFixedExposedPort(DEFAULT_PORT, port);
    } else {
      container.withExposedPorts(DEFAULT_PORT);
    }

    start(authenticationMode);
  }

  private FixedHostPortGenericContainer setupContainer(
      String username, String password, AuthenticationMode authenticationMode) {
    final FixedHostPortGenericContainer container =
        new FixedHostPortGenericContainer("bitnami/memcached:" + MEMCACHED_VERSION);

    switch (authenticationMode) {
      case NONE:
        break;
      case SASL:
        if (username != null && password != null) {
          container.withEnv("MEMCACHED_USERNAME", username);
          container.withEnv("MEMCACHED_PASSWORD", password);
        }
        break;
      case ASCII:
        container.withClasspathResourceMapping("/auth.file", "/tmp/auth.file", BindMode.READ_ONLY);
        container.setCommand("/opt/bitnami/scripts/memcached/run.sh -Y /tmp/auth.file");
        break;
    }

    return container;
  }

  private FixedHostPortGenericContainer setupTLSContainer() {
    final FixedHostPortGenericContainer container =
        new FixedHostPortGenericContainer("memcached:" + MEMCACHED_VERSION);

    container.withClasspathResourceMapping(
        "/pki/test.pem", "/test-certs/test.pem", BindMode.READ_ONLY);
    container.withClasspathResourceMapping(
        "/pki/test.key", "/test-certs/test.key", BindMode.READ_ONLY);

    container.withCommand(
        "memcached",
        "--enable-ssl",
        "-o",
        "ssl_verify_mode=3",
        "-o",
        "ssl_chain_cert=/test-certs/test.pem",
        "-o",
        "ssl_key=/test-certs/test.key",
        "-o",
        "ssl_ca_cert=/test-certs/test.pem");
    return container;
  }

  public void stop() {
    client.shutdown();
    try {
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
    client = null;
    container.stop();
  }

  public void start(AuthenticationMode authenticationMode) {
    if (!container.isRunning()) {
      container.start();
    }
    if (client == null) {
      final MemcacheClientBuilder<String> builder =
          MemcacheClientBuilder.newStringClient().withAddress(getHost(), getPort());
      if (username != null && password != null) {
        builder.withUsernamePassword(username, password);
      }

      if (useTLS) {
        try {
          builder.withSSLEngineFactory(new DefaultSSLEngineFactory(false));
        } catch (NoSuchAlgorithmException e) {
          throw new RuntimeException(e);
        }
      }

      switch (authenticationMode) {
        case NONE:
        case SASL:
          client = builder.connectBinary();
          break;
        case ASCII:
          client = builder.connectAscii();
          break;
      }

      try {
        client.awaitConnected(10, TimeUnit.SECONDS);
      } catch (InterruptedException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public int getPort() {
    return container.getMappedPort(DEFAULT_PORT);
  }

  public String getHost() {
    return container.getContainerIpAddress();
  }

  public void flush() {
    try {
      client.flushAll(0).toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public MemcacheClient<String> getClient() {
    return client;
  }
}

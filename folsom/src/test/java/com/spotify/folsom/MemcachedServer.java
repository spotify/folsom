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

import org.testcontainers.containers.BindMode;
import org.testcontainers.images.builder.Transferable;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
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
  private final String username;
  private final String password;

  public MemcachedServer() {
    this(null, null);
  }

  public MemcachedServer(String username, String password) {
    this(username, password, DEFAULT_PORT, AuthenticationMode.NONE);
  }

  public MemcachedServer(String username, String password, AuthenticationMode authenticationMode) {
    this(username, password, DEFAULT_PORT, authenticationMode);
  }

  public MemcachedServer(String username, String password, int port, AuthenticationMode authenticationMode) {
    this.username = username;
    this.password = password;
    container = new FixedHostPortGenericContainer("bitnami/memcached:1.6.21");
    if (port != DEFAULT_PORT) {
      container.withFixedExposedPort(DEFAULT_PORT, port);
    } else {
      container.withExposedPorts(DEFAULT_PORT);
    }
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
        container.setCommand("/opt/bitnami/scripts/memcached/run.sh -vvvv -Y /tmp/auth.file");
        break;
    }

    start(authenticationMode);
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

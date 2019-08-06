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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.testcontainers.containers.FixedHostPortGenericContainer;

public class MemcachedServer {

  private final FixedHostPortGenericContainer container;
  private MemcacheClient<String> client;

  public static final Supplier<MemcachedServer> SIMPLE_INSTANCE =
      Suppliers.memoize(MemcachedServer::new)::get;
  private final String username;
  private final String password;

  public MemcachedServer() {
    this(null, null);
  }

  public MemcachedServer(String username, String password) {
    this(username, password, Optional.empty());
  }

  public MemcachedServer(String username, String password, Optional<Integer> fixedPort) {
    this.username = username;
    this.password = password;
    container = new FixedHostPortGenericContainer("bitnami/memcached:1.5.12");
    if (fixedPort.isPresent()) {
      container.withFixedExposedPort(11211, fixedPort.get());
    } else {
      container.withExposedPorts(11211);
    }
    if (username != null && password != null) {
      container.withEnv("MEMCACHED_USERNAME", username);
      container.withEnv("MEMCACHED_PASSWORD", password);
    }
    start();
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

  public void start() {
    if (!container.isRunning()) {
      container.start();
    }
    if (client == null) {
      final MemcacheClientBuilder<String> builder =
          MemcacheClientBuilder.newStringClient().withAddress(getHost(), getPort());
      if (username != null && password != null) {
        builder.withUsernamePassword(username, password);
      }
      client = builder.connectBinary();
      try {
        client.awaitConnected(10, TimeUnit.SECONDS);
      } catch (InterruptedException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public int getPort() {
    return container.getMappedPort(11211);
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

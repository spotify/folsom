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

import com.spotify.folsom.authenticate.PlaintextAuthenticator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testcontainers.containers.GenericContainer;

public class MemcachedServer {

  private final GenericContainer container;
  private final MemcacheClient<String> client;

  public MemcachedServer() {
    this(null, null);

  }

  public MemcachedServer(String username, String password) {
    container = new GenericContainer("bitnami/memcached:1.5.10");
    container.addExposedPort(11211);
    if (username != null && password != null) {
      container.withEnv("MEMCACHED_USERNAME", username);
      container.withEnv("MEMCACHED_PASSWORD", password);
    }
    container.start();

    final MemcacheClientBuilder<String> builder = MemcacheClientBuilder.newStringClient()
        .withAddress(getHost(), getPort());
    if (username != null && password != null) {
      client = builder.connectBinary(new PlaintextAuthenticator(username, password));
    } else {
      client = builder.connectBinary();
    }
    try {
      client.awaitConnected(10, TimeUnit.SECONDS);
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    client.shutdown();
    try {
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
    container.stop();
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
}

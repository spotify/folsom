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
import java.io.File;
import java.security.NoSuchAlgorithmException;
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
  private final boolean secure;

  public MemcachedServer() {
    this(null, null);
  }

  public MemcachedServer(boolean secure) {
    this(null, null, Optional.empty(), secure);
  }

  public MemcachedServer(String username, String password) {
    this(username, password, Optional.empty(), false);
  }

  private MemcachedServer(
      String username, String password, Optional<Integer> fixedPort, boolean secure) {
    // Use self-signed test certs
    String currentDirectory = System.getProperty("user.dir");
    System.setProperty(
        "javax.net.ssl.keyStore", currentDirectory + "/src/test/resources/pki/test.p12");
    System.setProperty("javax.net.ssl.keyStoreType", "pkcs12");
    System.setProperty("javax.net.ssl.keyStorePassword", "changeit");
    System.setProperty(
        "javax.net.ssl.trustStore", currentDirectory + "/src/test/resources/pki/test.p12");
    System.setProperty("javax.net.ssl.trustStoreType", "pkcs12");
    System.setProperty("javax.net.ssl.trustStorePassword", "changeit");

    this.username = username;
    this.password = password;
    this.secure = secure;

    if (secure) {
      if (username != null || password != null) {
        throw new RuntimeException(
            "username and password are not currently supported with the secure container");
      }
      container = new FixedHostPortGenericContainer("memcached:1.6.15");

      // mount volume with test certs required for connecting
      File pkiVolumeFile = new File("src/test/resources/pki");
      container.withFileSystemBind(pkiVolumeFile.getAbsolutePath(), "/test-certs");

      // Run memcached with TLS
      container.withCommand(
          "memcached",
          "-vv",
          "-Z", // enable TLS
          "-o",
          "ssl_chain_cert=/test-certs/test.pem,"
              + // Use self-signed test certs
              "ssl_key=/test-certs/test.key,"
              + "ssl_verify_mode=3,"
              + "ssl_ca_cert=/test-certs/test.pem,"
              + "ssl_session_cache=shared:SSL:50m");
    } else {
      container = new FixedHostPortGenericContainer("bitnami/memcached:1.5.12");
    }
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

      if (secure) {
        try {

          builder.withSSLEngineFactory(new DefaultSSLEngineFactory(false));
        } catch (NoSuchAlgorithmException e) {
          throw new RuntimeException(e);
        }
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

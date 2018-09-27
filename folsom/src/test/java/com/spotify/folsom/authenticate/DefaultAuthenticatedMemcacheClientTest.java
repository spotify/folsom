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
package com.spotify.folsom.authenticate;

import static com.spotify.folsom.MemcacheStatus.OK;
import static com.spotify.hamcrest.future.CompletableFutureMatchers.stageWillCompleteWithValueThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.folsom.MemcacheAuthenticationException;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import com.spotify.folsom.MemcachedServer;
import com.spotify.folsom.Transcoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DefaultAuthenticatedMemcacheClientTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final String USERNAME = "theuser";
  private static final String PASSWORD = "a_nice_password";

  private static MemcachedServer server;

  @BeforeClass
  public static void setUpClass() {
    server = new MemcachedServer(USERNAME, PASSWORD);
  }

  @AfterClass
  public static void tearDownClass() {
    server.stop();
  }

  @Test
  public void testAuthenticateAndSet() throws InterruptedException, TimeoutException {
    Authenticator authenticator = new PlaintextAuthenticator(USERNAME, PASSWORD);
    MemcacheClient<String> client = new MemcacheClientBuilder<>(createProtobufTranscoder())
        .withAddress(server.getHost(), server.getPort())
        .connectBinary(authenticator);

    client.awaitConnected(20, TimeUnit.SECONDS);

    assertThat(client.set("some_key", "some_val", 1).toCompletableFuture(),
        stageWillCompleteWithValueThat(is(OK)));
  }

  @Test
  public void testFailedAuthentication() throws InterruptedException, TimeoutException, ExecutionException {
    Authenticator authenticator = new PlaintextAuthenticator(USERNAME, "wrong_password");
    MemcacheClient<String> client = new MemcacheClientBuilder<>(createProtobufTranscoder())
        .withAddress(server.getHost(), server.getPort())
        .connectBinary(authenticator);

    thrown.expect(MemcacheAuthenticationException.class);
    client.awaitConnected(20, TimeUnit.SECONDS);
  }

  @Test
  public void unAuthorizedBinaryClientFails() throws InterruptedException, ExecutionException, TimeoutException {
    MemcacheClient<String> client = new MemcacheClientBuilder<>(createProtobufTranscoder())
        .withAddress(server.getHost(), server.getPort())
        .connectBinary();

    thrown.expect(MemcacheAuthenticationException.class);
    client.awaitConnected(20, TimeUnit.SECONDS);
  }

  @Test
  public void unAuthorizedAsciiClientFails() throws InterruptedException, ExecutionException, TimeoutException {
    MemcacheClient<String> client = new MemcacheClientBuilder<>(createProtobufTranscoder())
        .withAddress(server.getHost(), server.getPort())
        .connectAscii();

    thrown.expect(MemcacheAuthenticationException.class);
    client.awaitConnected(20, TimeUnit.SECONDS);
  }

  private Transcoder<String> createProtobufTranscoder() {
    return new Transcoder<String>() {
      @Override
      public String decode(final byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
      }

      @Override
      public byte[] encode(final String entity) {
        return entity.getBytes(StandardCharsets.UTF_8);
      }
    };
  }

}

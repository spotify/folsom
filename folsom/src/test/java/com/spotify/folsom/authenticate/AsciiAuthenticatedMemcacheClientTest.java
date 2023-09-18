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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AsciiAuthenticatedMemcacheClientTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String USERNAME = "theuser";
  private static final String PASSWORD = "a_nice_password";
  private static final String WRONG_PASSWORD = "wrong_password";

  private static MemcachedServer asciiAuthServer;

  @BeforeClass
  public static void setUpClass() {
    asciiAuthServer =
        new MemcachedServer(USERNAME, PASSWORD, MemcachedServer.AuthenticationMode.ASCII);
  }

  @AfterClass
  public static void tearDownClass() {
    if (asciiAuthServer != null) {
      asciiAuthServer.stop();
    }
  }

  @Test
  public void testAuthenticateAndSet() throws InterruptedException, TimeoutException {
    testAsciiAuthenticationSuccess(asciiAuthServer);
  }

  private void testAsciiAuthenticationSuccess(final MemcachedServer server)
      throws TimeoutException, InterruptedException {
    MemcacheClient<String> client =
        MemcacheClientBuilder.newStringClient()
            .withAddress(server.getHost(), server.getPort())
            .withUsernamePassword(USERNAME, WRONG_PASSWORD)
            .withUsernamePassword(USERNAME, PASSWORD)
            .connectAscii();

    client.awaitConnected(20, TimeUnit.SECONDS);

    assertThat(
        client.set("some_key", "some_val", 1).toCompletableFuture(),
        stageWillCompleteWithValueThat(is(OK)));

    assertThat(
        client.get("some_key").toCompletableFuture(),
        stageWillCompleteWithValueThat(is("some_val")));
  }

  @Test
  public void testFailedAuthentication() throws InterruptedException, TimeoutException {
    MemcacheClient<String> client =
        MemcacheClientBuilder.newStringClient()
            .withAddress(asciiAuthServer.getHost(), asciiAuthServer.getPort())
            .withUsernamePassword(USERNAME, WRONG_PASSWORD)
            .withUsernamePassword(USERNAME, WRONG_PASSWORD)
            .connectAscii();

    thrown.expect(MemcacheAuthenticationException.class);
    client.awaitConnected(20, TimeUnit.SECONDS);
  }

  @Test
  public void unAuthorizedBinaryClientFails() throws InterruptedException, TimeoutException {
    MemcacheClient<String> client =
        MemcacheClientBuilder.newStringClient()
            .withAddress(asciiAuthServer.getHost(), asciiAuthServer.getPort())
            .connectBinary();

    thrown.expect(TimeoutException.class);
    client.awaitConnected(2, TimeUnit.SECONDS);
  }

  @Test
  public void unAuthorizedAsciiClientFails() throws InterruptedException, TimeoutException {
    MemcacheClient<String> client =
        MemcacheClientBuilder.newStringClient()
            .withAddress(asciiAuthServer.getHost(), asciiAuthServer.getPort())
            .connectAscii();

    thrown.expect(MemcacheAuthenticationException.class);
    client.awaitConnected(2, TimeUnit.SECONDS);
  }
}

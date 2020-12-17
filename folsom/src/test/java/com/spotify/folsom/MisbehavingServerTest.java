/*
 * Copyright (c) 2015 Spotify AB
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MisbehavingServerTest {
  private static final String HOST = "127.0.0.8";
  private Server server;

  @Before
  public void setup() throws Exception {}

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void testInvalidAsciiResponse() throws Throwable {
    testAsciiGet("HIPPO\r\n", "Unexpected line: HIPPO" + ", memcached node:" + HOST);
  }

  @Test
  public void testInvalidAsciiResponse2() throws Throwable {
    testAsciiGet("HIPPOS\r\n", "Unexpected line: HIPPOS" + ", memcached node:" + HOST);
  }

  @Test
  public void testInvalidAsciiResponse3() throws Throwable {
    testAsciiGet(
        "AAAAAAAAAAAAAAARGH\r\n",
        "Unexpected line: AAAAAAAAAAAAAAARGH" + ", memcached node:" + HOST);
  }

  @Test
  public void testAsciiNotANumber() throws Throwable {
    testAsciiGet("123ABC\r\n", "Unexpected line: 123ABC" + ", memcached node:" + HOST);
  }

  @Test
  public void testEmptyAsciiResponse() throws Throwable {
    testAsciiGet("\r\n", "Unexpected line: " + ", memcached node:" + HOST);
  }

  @Test
  public void testNotNewline() throws Throwable {
    testAsciiGet("\rFoo\n", "Expected newline, got something else" + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiGet() throws Throwable {
    testAsciiGet("VALUE\r\n", "Unexpected line: VALUE" + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiGet2() throws Throwable {
    testAsciiGet("VALUE \r\n", "Unexpected line: VALUE " + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiGet3() throws Throwable {
    testAsciiGet("VALUE key\r\n", "Unexpected line: VALUE key" + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiGet4() throws Throwable {
    testAsciiGet(
        "VALUE key 123\r\n", "Unexpected line: VALUE key 123" + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiGet5() throws Throwable {
    testAsciiGet("VALUE key 123 456\r\n", "Timeout" + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiGet6() throws Throwable {
    testAsciiGet(
        "VALUE key 123 0\r\nfoo\r\n",
        "Unexpected end of data block: foo" + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiGet7() throws Throwable {
    testAsciiGet(
        "VALUE key 123 0\r\n\r\nSTORED\r\n",
        "Unexpected line: STORED" + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiGet8() throws Throwable {
    testAsciiGet(
        "VALUE key 123 1a3\r\n", "Unexpected line: VALUE key 123 1a3" + ", memcached node:" + HOST);
  }

  @Test
  public void testWrongAsciiKey() throws Throwable {
    testAsciiGet(
        "VALUE otherkey 123 0\r\n\r\nEND\r\n",
        "Expected key key but got otherkey" + ", memcached node:" + HOST);
  }

  @Test
  public void testTooManyAsciiValues() throws Throwable {
    testAsciiGet(
        "" + "VALUE key 123 0\r\n" + "\r\n" + "VALUE key 123 0\r\n" + "\r\n" + "END\r\n",
        "Too many responses, expected 1 but got 2" + ", memcached node:" + HOST);
  }

  @Test
  public void testAsciiWrongResponseType() throws Throwable {
    testAsciiGet(
        "1234\r\n", "Unexpected response type: NUMERIC_VALUE" + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiTouch() throws Throwable {
    testAsciiTouch("STORED\r\n", "Unexpected line: STORED" + ", memcached node:" + HOST);
  }

  @Test
  public void testBadAsciiSet() throws Throwable {
    testAsciiSet("TOUCHED\r\n", "Unexpected line: TOUCHED" + ", memcached node:" + HOST);
  }

  private void testAsciiGet(String response, String expectedError) throws Exception {
    MemcacheClient<String> client = setupAscii(response);
    try {
      client.get("key").toCompletableFuture().get();
      fail();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertEquals(MemcacheClosedException.class, cause.getClass());
      assertEquals(expectedError, cause.getMessage());
    } finally {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  private void testAsciiTouch(String response, String expectedError) throws Exception {
    MemcacheClient<String> client = setupAscii(response);
    try {
      client.touch("key", 123).toCompletableFuture().get();
      fail();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertEquals(MemcacheClosedException.class, cause.getClass());
      assertEquals(expectedError, cause.getMessage());
    } finally {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  private void testAsciiSet(String response, String expectedError) throws Exception {
    MemcacheClient<String> client = setupAscii(response);
    try {
      client.set("key", "value", 123).toCompletableFuture().get();
      fail();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertEquals(MemcacheClosedException.class, cause.getClass());
      assertEquals(expectedError, cause.getMessage());
    } finally {
      client.shutdown();
      client.awaitDisconnected(10, TimeUnit.SECONDS);
    }
  }

  private MemcacheClient<String> setupAscii(String response) throws Exception {
    server = new Server(response);
    MemcacheClient<String> client =
        MemcacheClientBuilder.newStringClient()
            .withAddress(HOST, server.port)
            .withRequestTimeoutMillis(100L)
            .withRetry(false)
            .connectAscii();
    client.awaitConnected(10, TimeUnit.SECONDS);
    return client;
  }

  private static class Server {
    private final int port;
    private final ServerSocket serverSocket;
    private final Thread thread;

    private volatile Throwable failure;
    private volatile Socket socket;

    private Server(String responseString) throws IOException {
      final byte[] response = responseString.getBytes(StandardCharsets.UTF_8);
      serverSocket = new ServerSocket(0);
      port = serverSocket.getLocalPort();
      thread =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    socket = serverSocket.accept();
                    handleConnection(socket);
                  } catch (Throwable e) {
                    failure = e;
                    failure.printStackTrace();
                  }
                }

                private void handleConnection(Socket socket) throws Exception {
                  BufferedReader reader =
                      new BufferedReader(new InputStreamReader(socket.getInputStream()), 1);
                  String s;
                  while (true) {
                    s = reader.readLine();
                    if (s.equals("get folsom_authentication_validation")) {
                      // Handle authentication phase first
                      socket.getOutputStream().write("END\r\n".getBytes(StandardCharsets.UTF_8));
                      socket.getOutputStream().flush();
                    } else {
                      break;
                    }
                  }
                  if (s.startsWith("get ") || s.startsWith("touch ")) {
                    // Don't need to read any more lines
                  } else if (s.startsWith("set ")) {
                    // Read the value too
                    reader.readLine();
                  } else {
                    throw new RuntimeException("Unhandled command: " + s);
                  }
                  socket.getOutputStream().write(response);
                  socket.getOutputStream().flush();
                }
              });
      thread.setName("misbehaving-server-thread-" + port);
      thread.start();
    }

    public void stop() throws Exception {
      thread.join();
      serverSocket.close();
      if (socket != null) {
        socket.close();
      }
      if (failure != null) {
        fail(failure.getClass().getSimpleName() + ": " + failure.getMessage());
      }
    }
  }
}

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

package com.spotify.folsom.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SlowStaticServer implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(SlowStaticServer.class);

  private final ScheduledExecutorService executor;
  private final byte[] response;
  private final int delayMillis;

  private volatile boolean shutdown = false;
  private ServerSocket serverSocket;

  public SlowStaticServer(byte[] response, int delayMillis) {
    this.executor = Executors.newScheduledThreadPool(10);
    this.response = response;
    this.delayMillis = delayMillis;
  }

  public int start(int listenPort) throws IOException {
    serverSocket = new ServerSocket(listenPort);
    executor.execute(new Listener());
    return serverSocket.getLocalPort();
  }

  @Override
  public void close() {
    if (!shutdown) {
      shutdown = true;
      executor.shutdownNow();
      try {
        serverSocket.close();
      } catch (IOException ignored) {
      }
    }
  }

  private class Listener implements Runnable {

    @Override
    public void run() {
      try {
        // expect just one connection
        final Socket socket = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        while (!shutdown) {
          String line = reader.readLine();
          if (!line.startsWith("get ")) {
            throw new RuntimeException("Unimplemented command: " + line);
          }
          executor.schedule(() -> {
            try {
              socket.getOutputStream().write(response);
              socket.getOutputStream().flush();
              log.debug("sent response");
            } catch (IOException e) {
              log.error("exception with socket", e);
            }
          }, delayMillis, TimeUnit.MILLISECONDS);
        }
      } catch (IOException e) {
        log.debug("shutting down due to error", e);
        close();
      }
    }
  }
}

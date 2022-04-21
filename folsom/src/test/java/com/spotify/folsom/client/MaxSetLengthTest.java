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
package com.spotify.folsom.client;

import static org.junit.Assert.fail;

import com.spotify.folsom.*;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

/**
 * This test makes sure that we don't send too large items to memcached server, to avoid server
 * errors and unnecessary reconnections. For some reason there is a discrepancy between the request
 * size we calculate and the max enforced by memcached, hence the need of the magic number in method
 * DefaultRawMemcacheClient#send. The max item size actually enforced by memcached is slightly
 * smaller than the argument given at server start, and it varies depending on binary/ascii protocol
 * and the value of max item size which with the server was started. This test starts memcached
 * servers with different values of max item size and sends SET requests that are too large,
 * decreasing them byte by byte, until the request is successful, validating that we never send a
 * request too large to the server, request sizes are limited in the client.
 *
 * @see DefaultRawMemcacheClient#send
 */
public class MaxSetLengthTest {

  @Test
  public void testMaxItemSizes() throws Exception {
    double[] maxItemSizeFactors = new double[] {0.5, 1, 2, 4};

    for (double factor : maxItemSizeFactors) {
      int maxItemSize = (int) (1024 * 1024 * factor);

      MemcachedServer server = startMemcached(maxItemSize);

      AsciiMemcacheClient<String> asciiClient = clientBuilder(server, maxItemSize).connectAscii();
      testMaxItemSize(asciiClient, maxItemSize);
      asciiClient.shutdown();

      BinaryMemcacheClient<String> binaryClient =
          clientBuilder(server, maxItemSize).connectBinary();
      testMaxItemSize(binaryClient, maxItemSize);
      binaryClient.shutdown();

      server.stop();
    }
  }

  MemcachedServer startMemcached(int maxItemSize) {
    return new MemcachedServer(null, null, Optional.empty(), Optional.of(maxItemSize));
  }

  MemcacheClientBuilder<String> clientBuilder(MemcachedServer server, int maxItemSize) {
    return MemcacheClientBuilder.<String>newSerializableObjectClient()
        .withAddress(server.getHost(), server.getPort())
        .withMaxSetLength(maxItemSize);
  }

  void testMaxItemSize(MemcacheClient<String> client, int maxItemSize) throws Exception {
    ConnectFuture.connectFuture(client).toCompletableFuture().get();
    boolean ok = false;
    for (int itemSize = maxItemSize; !ok; itemSize--) {
      String value = getValue(itemSize);
      MemcacheStatus status = null;
      try {
        status = client.set("somekey", value, 1000).toCompletableFuture().get();
      } catch (ExecutionException e) {
        fail(e.getCause().toString());
      }
      ok = MemcacheStatus.OK == status;
    }
  }

  String getValue(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 42);
    return new String(bytes);
  }
}

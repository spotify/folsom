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

package com.spotify.folsom;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.transcoder.StringTranscoder;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KetamaRunner {

  public static void main(final String[] args) throws Throwable {
    ImmutableList<HostAndPort> addresses = ImmutableList.of(
            HostAndPort.fromParts("127.0.0.1", 11211),
            HostAndPort.fromParts("127.0.0.1", 11213));
    final BinaryMemcacheClient<String> client =
            new MemcacheClientBuilder<>(StringTranscoder.UTF8_INSTANCE)
                    .withAddresses(addresses)
                    .connectBinary();

    for (int i = 0; i<10; i++) {
      final String key = "key" + i;
      final String value = "value" + i;
      checkKeyOkOrNotFound(client.delete(key));

      client.set(key, value, 1000);

      System.out.println(client.get(key).get());
    }

    client.shutdown();
  }

  private static void checkKeyOkOrNotFound(final ListenableFuture<?> future) throws Throwable {
    checkStatus(future, ImmutableSet.of(MemcacheStatus.KEY_NOT_FOUND, MemcacheStatus.OK));
  }

  private static void checkStatus(final ListenableFuture<?> future, final Set<MemcacheStatus> expected)
      throws Throwable {
    try {
      final Object v = future.get();
      if (v == null && expected.contains(MemcacheStatus.KEY_NOT_FOUND)) {
        // ok
      } else if (v != null && expected.contains(MemcacheStatus.OK)) {
        // ok
      } else {
        throw new IllegalStateException();
      }
    } catch (final ExecutionException e) {
      throw e.getCause();
    }
  }
}

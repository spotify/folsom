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

import com.google.common.collect.Lists;
import com.spotify.folsom.transcoder.StringTranscoder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class LoadTestRunner {

  public static void main(final String[] args) throws Throwable {
    final BinaryMemcacheClient<String> client =
            new MemcacheClientBuilder<>(StringTranscoder.UTF8_INSTANCE)
                    .withAddress("127.0.0.1")
                    .withMaxOutstandingRequests(100000)
                    .connectBinary();

    final String[] keys = new String[10];
    for (int i = 0; i < 10; i++) {
      keys[i] = "key" + i;
    }

    final List<CompletionStage<Boolean>> futures = Lists.newArrayList();

    for (int r = 0; r < 100; r++) {
      for (final String keyProto : keys) {
        final String key = keyProto + ":" + r;
        final String input = "value" + key;
        futures.add(client.set(key, input, 100000)
            .thenCompose(__ -> client.get(key))
            .thenCompose(value -> client.delete(key)
                .thenApply(__ -> (input).equals(value))));
      }
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    long failures = futures.stream()
        .map(cs -> cs.toCompletableFuture().join())
        .filter(r -> r.booleanValue())
        .count();

    System.out.println(failures + " failed of " + futures.size());
    client.shutdown();
  }
}

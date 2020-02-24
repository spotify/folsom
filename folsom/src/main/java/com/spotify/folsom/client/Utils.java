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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

public final class Utils {

  private static final int TTL_CUTOFF = 60 * 60 * 24 * 30;

  private Utils() {}

  public static int ttlToExpiration(final int ttl) {
    // This intentionally includes negative and zero TTLs
    if (ttl < TTL_CUTOFF) {
      return ttl;
    }

    // Above the cutoff it needs to be translated to a seconds-since-epoch timestamp
    long expirationTime = (System.currentTimeMillis() / 1000) + ttl;
    if ((int) expirationTime != expirationTime) {
      // The above check is the same as performed by toIntExact, signifying an integer overflow.
      // Not strictly correct - should switch to failure on the next major version bump
      return Integer.MAX_VALUE
          - 1; // Avoid Integer.MAX_VALUE in case memcached treats it in some special way.
    }

    return (int) expirationTime;
  }

  public static <T> Function<List<List<T>>, List<T>> flatten() {
    return input -> Lists.newArrayList(Iterables.concat(input));
  }

  /** A counter of all currently connected clients. This can be useful to detect connection leaks */
  public static int getGlobalConnectionCount() {
    return DefaultRawMemcacheClient.getGlobalConnectionCount();
  }

  public static Throwable unwrap(final Throwable e) {
    if (e instanceof ExecutionException || e instanceof CompletionException) {
      return unwrap(e.getCause());
    }
    return e;
  }

  static <T> CompletionStage<T> onExecutor(CompletionStage<T> future, Executor executor) {
    if (executor == null) {
      return future;
    }
    return future.whenCompleteAsync((t, throwable) -> {}, executor);
  }
}

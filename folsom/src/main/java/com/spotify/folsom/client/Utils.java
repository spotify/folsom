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
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public final class Utils {

  private static final int TTL_CUTOFF = 60 * 60 * 24 * 30;

  private Utils() {}

  public static int ttlToExpiration(final int ttl) {
    if (ttl <= 0) {
      return 0;
    }

    if (ttl < TTL_CUTOFF) {
      return ttl;
    }

    int expirationTime = (int) (System.currentTimeMillis() / 1000) + ttl;
    if (expirationTime < 0) {
      // throw new IllegalArgumentException("TTL set too far into the future (Y2038 limitation)");
      // Not strictly correct - should switch to failure on the next major version bump
      return Integer.MAX_VALUE;
    }
    return expirationTime;
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
}

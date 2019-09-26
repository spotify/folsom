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

import static java.util.Objects.requireNonNull;

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
      return Integer.MAX_VALUE
          - 1; // Avoid Integer.MAX_VALUE in case memcached treats it in some special way.
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

  static <T> CompletionStage<T> onExecutor(CompletionStage<T> future, Executor executor) {
    if (executor == null) {
      return future;
    }
    return future.whenCompleteAsync((t, throwable) -> {}, executor);
  }

  /**
   * Checks if an executor is direct or not. Direct means that the executor executes work on the
   * same thread as the work was submitted on.
   *
   * @param executor may not be null
   * @return true if the executor is a direct executor, otherwise false
   */
  static boolean isDirectExecutor(Executor executor) {
    requireNonNull(executor);
    RunnableWithThread runnableWithThread = new RunnableWithThread();
    try {
      executor.execute(runnableWithThread);

      // We have the following cases
      // 1) It is a direct executor, so runnableWithThread.thread will be set to the current thread.
      //    We will correctly return true
      // 2) It is not a direct executor and runnableWithThread.thread is still null when we check
      // it.
      //    We correctly return false
      // 3) It is not a direct executor but the runnableWithThread.thread somehow managed to get set
      //    before we check it. In any case, it can't be referencing the same thread we are in
      //    We correctly return false

      return runnableWithThread.thread == Thread.currentThread();
    } catch (Exception e) {
      // If the executor throws any exception, it can not be a direct executor
      return false;
    }
  }

  private static class RunnableWithThread implements Runnable {
    private Thread thread;

    @Override
    public void run() {
      thread = Thread.currentThread();
    }
  }
}

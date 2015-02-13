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

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

public final class Utils {

  private static final Logger log = LoggerFactory.getLogger(Utils.class);


  /**
   * The Guava {@link com.google.common.util.concurrent.MoreExecutors#sameThreadExecutor()}
   * takes locks, so roll our own.
   */
  public static final Executor SAME_THREAD_EXECUTOR = new Executor() {
    @Override
    public void execute(final Runnable command) {
      try {
        command.run();
      } catch (final Exception e) {
        log.error("caught exception", e);
      }
    }
  };

  public static <I, O> ListenableFuture<O> transform(
          final ListenableFuture<I> input,
          final AsyncFunction<? super I, ? extends O> function) {
    return Futures.transform(input, function, SAME_THREAD_EXECUTOR);
  }

  public static <I, O> ListenableFuture<O> transform(
          final ListenableFuture<I> input,
          final Function<? super I, ? extends O> function) {
    return Futures.transform(input, function, SAME_THREAD_EXECUTOR);
  }

  private Utils() {
  }

  public static int ttlToExpiration(final int ttl) {
    return (int) (System.currentTimeMillis() / 1000) + ttl;
  }

  public static void writeKeyString(final ByteBuffer buffer, final String key) {
    final int length = key.length();
    for (int i = 0; i < length; i++) {
      final char c = key.charAt(i);
      buffer.put((byte) c);
    }
  }

  public static void validateKey(final String key) {
    final int length = key.length();
    if (length > 250) {
      throw new IllegalArgumentException("Invalid key: " + key);
    }
    for (int i = 0; i < length; i++) {
      final char c = key.charAt(i);
      if (c <= 32 || c > 127) {
        throw new IllegalArgumentException("Invalid key: " + key);
      }
    }
  }
}

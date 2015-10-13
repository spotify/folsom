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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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

  /**
   * Convert a plain TTL value to a memcached expiration field
   * @param ttl a non-negative integer representing the TTL in seconds
   * @return a memcached expiration field value,
   *         which can be either 0 (never expire, a ttl in seconds, or a unix timestamp)
   */
  public static int ttlToExpiration(final int ttl) {
    if (ttl <= 0) {
      return 0;
    }
    if (ttl < 60 * 60 * 24 * 30) {
      return ttl;
    }

    return (int) (System.currentTimeMillis() / 1000) + ttl;
  }

  public static <T> Function<List<List<T>>, List<T>> flatten() {
    return new Function<List<List<T>>, List<T>>() {
      @Override
      public List<T> apply(final List<List<T>> input) {
        return Lists.newArrayList(Iterables.concat(input));
      }
    };
  }

}

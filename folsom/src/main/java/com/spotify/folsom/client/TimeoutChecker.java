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

import java.util.concurrent.TimeUnit;

/**
 * A utility for checking whether some state has changed within a specified timout. Could be used
 * to verify that the head of a request queue changes quickly enough, indicating forward progress.
 */
class TimeoutChecker<T> {

  private final long timeoutNanos;

  private T pending;
  private long timestamp;

  public TimeoutChecker(final TimeUnit unit, final long timeout) {
    this.timeoutNanos = unit.toNanos(timeout);
  }

  public boolean check(final T current) {
    final long nowNanos = System.nanoTime();

    // New task?
    if (current != pending) {
      pending = current;
      timestamp = nowNanos;
      return false;
    }

    // Timed out?
    return nowNanos - timestamp > timeoutNanos;
  }

  public static <T> TimeoutChecker<T> create(final TimeUnit unit, final long timeout) {
    return new TimeoutChecker<>(unit, timeout);
  }
}

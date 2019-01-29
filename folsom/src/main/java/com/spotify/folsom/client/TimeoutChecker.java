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

/** A utility for checking whether a {@link Request} has timed out. */
class TimeoutChecker {

  private final long timeoutNanos;

  private TimeoutChecker(final TimeUnit unit, final long timeout) {
    timeoutNanos = unit.toNanos(timeout);
  }

  public boolean check(final Request<?> request) {
    return elapsedNanos(request) > timeoutNanos;
  }

  public long elapsedNanos(final Request<?> request) {
    return System.nanoTime() - request.getCreatedNanos();
  }

  public static TimeoutChecker create(final TimeUnit unit, final long timeout) {
    return new TimeoutChecker(unit, timeout);
  }
}

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


public class ExponentialBackoff implements BackoffFunction {

  private final long minTime;
  private final long maxTime;
  private final double factor;

  public ExponentialBackoff(final long minTime, final long maxTime, final double factor) {
    this.minTime = minTime;
    this.maxTime = maxTime;
    this.factor = factor;
  }

  @Override
  public long getBackoffTimeMillis(final int reconnectAttempt) {
    if (reconnectAttempt <= 0) {
      return 0;
    }

    return (long) Math.min(maxTime, minTime * Math.pow(factor, reconnectAttempt - 1));
  }
}

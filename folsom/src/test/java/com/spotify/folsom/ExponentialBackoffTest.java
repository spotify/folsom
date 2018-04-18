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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExponentialBackoffTest {

  public static final double EPSILON = 0.001;

  @Test
  public void testSimple() {
    int minTime = 10;
    int maxTime = 1000;
    double factor = 2.0;
    ExponentialBackoff exponentialBackoff = new ExponentialBackoff(minTime, maxTime, factor);

    assertEquals(0, exponentialBackoff.getBackoffTimeMillis(-1));
    assertEquals(0, exponentialBackoff.getBackoffTimeMillis(0));
    assertEquals(minTime, exponentialBackoff.getBackoffTimeMillis(1));
    assertEquals(minTime * factor, exponentialBackoff.getBackoffTimeMillis(2), EPSILON);
    assertEquals(minTime * factor * factor, exponentialBackoff.getBackoffTimeMillis(3), EPSILON);
    assertEquals(maxTime, exponentialBackoff.getBackoffTimeMillis(10), EPSILON);

  }
}

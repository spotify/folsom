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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UtilsTest {

  @Test
  public void testTtlToExpirationSmallValues() throws Exception {
    assertEquals(0, Utils.ttlToExpiration(0));
    assertEquals(10, Utils.ttlToExpiration(10));
    assertEquals(100, Utils.ttlToExpiration(100));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTtlToExpirationNegative() throws Exception {
    Utils.ttlToExpiration(-1);
  }

  @Test
  public void testTtlToExpirationToTimestamp() throws Exception {
    int expectedTTL = 100 * 1000 * 1000;
    int expiration = Utils.ttlToExpiration(expectedTTL);

    final int now = (int) (System.currentTimeMillis() / 1000);
    final int actualTTL = expiration - now;

    assertEquals(expectedTTL, actualTTL, 1.0);
  }

  @Test
  public void testTtlToExpirationOverflow() throws Exception {
    assertEquals(Integer.MAX_VALUE - 1, Utils.ttlToExpiration(Integer.MAX_VALUE - 100000));
  }

  @Test
  public void testTtlToExpirationMaxValue() throws Exception {
    assertEquals(Integer.MAX_VALUE - 1, Utils.ttlToExpiration(Integer.MAX_VALUE));
  }
}

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
package com.spotify.folsom;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GetResultTest {

  @Test(expected = NullPointerException.class)
  public void testCstrNullValue() {
    GetResult.success(null, 123);
  }

  @Test
  public void testEqualsHashCode() {
    GetResult<Long> result1 = GetResult.success(123L, 456);
    GetResult<Long> result2 = GetResult.success(123L, 456);
    GetResult<Long> result3 = GetResult.success(999L, 999);
    GetResult<Long> result4 = GetResult.success(123L, 999);
    GetResult<Long> result5 = GetResult.success(999L, 456);

    assertTrue(result1.equals(result1));
    assertTrue(result1.equals(result2));
    assertTrue(result2.equals(result1));
    assertFalse(result1.equals(result3));
    assertFalse(result1.equals(result4));
    assertFalse(result1.equals(result5));
    assertFalse(result1.equals(null));
    assertFalse(result1.equals("dummy"));

    assertTrue(result1.hashCode() == result2.hashCode());
    assertFalse(result1.hashCode() == result3.hashCode());
    assertFalse(result1.hashCode() == result4.hashCode());
    assertFalse(result1.hashCode() == result5.hashCode());
  }
}

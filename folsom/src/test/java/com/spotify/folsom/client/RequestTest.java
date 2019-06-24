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

import com.google.common.base.Strings;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class RequestTest {

  @Test
  public void testValidateKey() {
    AbstractRequest.encodeKey("hello", StandardCharsets.UTF_8, MemcacheEncoder.MAX_KEY_LEN);
  }

  @Test
  public void testValidateUTFCharacter() {
    AbstractRequest.encodeKey("räksmörgås", StandardCharsets.UTF_8, MemcacheEncoder.MAX_KEY_LEN);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateKeyTooLongKey() {
    AbstractRequest.encodeKey(
        Strings.repeat("hello", 100), StandardCharsets.UTF_8, MemcacheEncoder.MAX_KEY_LEN);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateKeyWithSpace() {
    AbstractRequest.encodeKey("hello world", StandardCharsets.UTF_8, MemcacheEncoder.MAX_KEY_LEN);
  }
}

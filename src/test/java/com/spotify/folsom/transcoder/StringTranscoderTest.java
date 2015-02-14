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
package com.spotify.folsom.transcoder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.base.Charsets;

public class StringTranscoderTest {

  private static final String STRING = "hällo wörld";
  private static final byte[] BYTES = STRING.getBytes(Charsets.UTF_8);
  private StringTranscoder transcoder = new StringTranscoder(Charsets.UTF_8);

  @Test
  public void testDecodeEncode() {
    assertEquals(STRING, transcoder.decode(BYTES));
    assertArrayEquals(BYTES, transcoder.encode(STRING));
  }
}

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

import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class SerializableObjectTranscoderTest {

  private static final class SerializableTestObject implements Serializable {
    private static final long serialVersionUID = 6961370831727465915L;
    private String value1;
    private String value2;
    private int value3;
  }

  @Test
  public void testEncodeDecode() {
    SerializableTestObject b = new SerializableTestObject();
    b.value1 = "hello";
    b.value2 = "world";
    b.value3 = 5;

    final byte[] encoded = SerializableObjectTranscoder.INSTANCE.encode(b);
    final SerializableTestObject testObject =
            (SerializableTestObject) SerializableObjectTranscoder.INSTANCE.decode(encoded);

    assertEquals("hello", testObject.value1);
    assertEquals("world", testObject.value2);
    assertEquals(5, testObject.value3);
  }
}

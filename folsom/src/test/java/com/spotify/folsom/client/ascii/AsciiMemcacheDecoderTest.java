/*
 * Copyright (c) 2026 Spotify AB
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

package com.spotify.folsom.client.ascii;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class AsciiMemcacheDecoderTest {

  @Test
  public void decodesMultipleValuesIncludingEmptyValueAndCas() {
    EmbeddedChannel channel = new EmbeddedChannel(new AsciiMemcacheDecoder(StandardCharsets.UTF_8));
    try {
      channel.writeInbound(
          Unpooled.copiedBuffer(
              "VALUE alpha 12 5 123\r\nhello\r\nVALUE beta 0 0\r\n\r\nEND\r\n",
              StandardCharsets.UTF_8));

      ValueAsciiResponse response = channel.readInbound();
      assertEquals(2, response.values.size());
      assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), response.values.get(0).key);
      assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), response.values.get(0).value);
      assertEquals(12, response.values.get(0).flags);
      assertEquals(123, response.values.get(0).cas);
      assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8), response.values.get(1).key);
      assertArrayEquals(new byte[0], response.values.get(1).value);
      assertEquals(0, response.values.get(1).cas);
      assertNull(channel.readInbound());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  public void decodesFragmentedHeaderValueAndTerminator() {
    EmbeddedChannel channel = new EmbeddedChannel(new AsciiMemcacheDecoder(StandardCharsets.UTF_8));
    try {
      for (String fragment :
          new String[] {"VALUE fra", "gmented 7 5\r", "\nhe", "llo\r", "\nEND\r", "\n"}) {
        channel.writeInbound(Unpooled.copiedBuffer(fragment, StandardCharsets.UTF_8));
      }

      ValueAsciiResponse response = channel.readInbound();
      assertEquals(1, response.values.size());
      assertArrayEquals("fragmented".getBytes(StandardCharsets.UTF_8), response.values.get(0).key);
      assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), response.values.get(0).value);
      assertEquals(7, response.values.get(0).flags);
      assertNull(channel.readInbound());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  public void decodesStatsWithUtf8Tokens() {
    EmbeddedChannel channel = new EmbeddedChannel(new AsciiMemcacheDecoder(StandardCharsets.UTF_8));
    try {
      channel.writeInbound(
          Unpooled.copiedBuffer(
              "STAT misses 20\r\nSTAT café déjà\r\nEND\r\n", StandardCharsets.UTF_8));

      StatsAsciiResponse response = channel.readInbound();
      assertEquals("20", response.values.get("misses"));
      assertEquals("déjà", response.values.get("café"));
      assertNull(channel.readInbound());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  public void decodesScalarRepliesAndResetsAfterEmptyGet() {
    EmbeddedChannel channel = new EmbeddedChannel(new AsciiMemcacheDecoder(StandardCharsets.UTF_8));
    try {
      channel.writeInbound(
          Unpooled.copiedBuffer("STORED\r\nEND\r\n12345\r\nNOT_FOUND\r\n", StandardCharsets.UTF_8));

      assertSame(AsciiResponse.STORED, channel.readInbound());
      assertSame(AsciiResponse.EMPTY_LIST, channel.readInbound());
      NumericAsciiResponse numeric = channel.readInbound();
      assertEquals(12345, numeric.numericValue);
      assertSame(AsciiResponse.NOT_FOUND, channel.readInbound());
      assertNull(channel.readInbound());
    } finally {
      channel.finishAndReleaseAll();
    }
  }
}

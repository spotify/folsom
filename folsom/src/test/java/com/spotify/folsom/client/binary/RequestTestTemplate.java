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

package com.spotify.folsom.client.binary;

import com.google.common.base.Charsets;
import com.spotify.folsom.transcoder.StringTranscoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public abstract class RequestTestTemplate {

  protected static final int ID = 17;
  protected static final StringTranscoder TRANSCODER = new StringTranscoder(Charsets.UTF_8);

  protected static final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

  static {
    when(ctx.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
  }

  protected void assertHeader(final ByteBuf b,
                              final int opcode,
                              final int keyLength,
                              final int extrasLength,
                              final int totalLength,
                              final int opaque,
                              final long cas) {
    assertByte(0x80, b.readByte());
    assertByte(opcode, b.readByte());
    assertShort(keyLength, b.readShort());
    assertShort(extrasLength, b.readByte());
    assertZeros(b, 3);
    assertEquals(totalLength, b.readInt());
    assertEquals(opaque, b.readInt());
    assertEquals(cas, b.readLong());
  }

  protected void assertByte(final int expected, final byte actual) {
    assertEquals((byte) expected, actual);
  }

  protected void assertShort(final int expected, final short actual) {
    assertEquals((short) expected, actual);
  }

  protected void assertZeros(final ByteBuf buf, final int len) {
    final byte[] zs = new byte[len];

    assertBytes(buf, zs);
  }

  protected void assertString(final String expected, final ByteBuf buf) {
    final byte[] es = expected.getBytes();
    assertBytes(buf, es);
  }

  protected void assertBytes(final ByteBuf buf, final byte[] zs) {
    final byte[] bs = new byte[zs.length];
    buf.readBytes(bs);
    assertArrayEquals(zs, bs);
  }

  protected void assertEOM(final ByteBuf b) {
    assertEquals(b.readerIndex(), b.writerIndex());
  }

  protected void assertExpiration(final int expiration) {
    final long now = System.currentTimeMillis() / 1000;

    // check if in a reasonable interval
    // TODO improve
    assertTrue(expiration > now - 10000);
    assertTrue(expiration < now + 10000);
  }
}

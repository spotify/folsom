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

import com.spotify.folsom.MemcacheStatus;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.util.List;

public class BinaryMemcacheDecoder extends ByteToMessageDecoder {

  private static final byte[] NO_BYTES = new byte[0];
  private static final int BATCH_SIZE = 16;

  private BinaryResponse replies = new BinaryResponse();

  public BinaryMemcacheDecoder() {
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf buf,
                        final List<Object> out) throws Exception {
    for (int i = 0; i < BATCH_SIZE; i++) {
      if (buf.readableBytes() < 24) {
        return;
      }

      buf.markReaderIndex();

      final int magicNumber = buf.readUnsignedByte(); // byte 0
      if (magicNumber != 0x81) {
        throw fail(buf, String.format("Invalid magic number: 0x%2x", magicNumber));
      }

      final int opcode = buf.readByte(); // byte 1
      final int keyLength = buf.readUnsignedShort(); // byte 2-3
      final int extrasLength = buf.readUnsignedByte(); // byte 4
      buf.skipBytes(1);
      final int statusCode = buf.readUnsignedShort(); // byte 6-7

      final MemcacheStatus status = MemcacheStatus.fromInt(statusCode);

      final int totalLength = buf.readInt(); // byte 8-11
      final int opaque = buf.readInt();

      final long cas = buf.readLong();

      if (buf.readableBytes() < totalLength) {
        buf.resetReaderIndex();
        return;
      }

      buf.skipBytes(extrasLength);

      buf.skipBytes(keyLength);

      final int valueLength = totalLength - keyLength - extrasLength;
      byte[] valueBytes;
      if (valueLength == 0) {
        valueBytes = NO_BYTES;
      } else {
        valueBytes = new byte[valueLength];
      }

      buf.readBytes(valueBytes);

      replies.add(new ResponsePacket((byte) opcode, status, opaque, cas, valueBytes));
      if ((opaque & 0xFF) == 0) {
        out.add(replies);
        replies = new BinaryResponse();
      }
    }
  }

  private IOException fail(final ByteBuf buf, final String message) {
    buf.resetReaderIndex();
    return new IOException(message);
  }

}

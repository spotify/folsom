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
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.client.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SetRequest
        extends BinaryRequest<MemcacheStatus>
        implements com.spotify.folsom.client.SetRequest {

  private final byte opcode;
  private final byte[] value;
  private final int ttl;
  private final long cas;

  public SetRequest(final byte opcode,
                    final byte[] key,
                    final byte[] value,
                    final int ttl,
                    final long cas) {
    super(key);
    this.opcode = opcode;
    this.value = value;
    this.ttl = ttl;
    this.cas = cas;
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    final int expiration = Utils.ttlToExpiration(ttl);

    final int valueLength = value.length;

    final boolean hasExtra =
            (opcode == OpCode.SET || opcode == OpCode.ADD || opcode == OpCode.REPLACE);

    final int extraLength = hasExtra ? 8 : 0;

    writeHeader(dst, opcode, extraLength, valueLength, cas);
    if (hasExtra) {
      dst.putInt(0); // byte 24-27, flags
      dst.putInt(expiration); // byte 28-31, expiration
    }
    dst.put(key);
    if (dst.remaining() >= valueLength) {
      dst.put(value);
      return toBuffer(alloc, dst);
    } else {
      return toBufferWithValue(alloc, dst, value);
    }
  }

  private static ByteBuf toBufferWithValue(final ByteBufAllocator alloc, ByteBuffer dst,
                                           byte[] value) {
    ByteBuf buffer = toBuffer(alloc, dst, value.length);
    buffer.writeBytes(value);
    return buffer;
  }

  @Override
  public void handle(final BinaryResponse replies) throws IOException {
    ResponsePacket reply = handleSingleReply(replies);

    if (OpCode.getKind(reply.opcode) != OpCode.SET) {
      throw new IOException("Unmatched response");
    }
    MemcacheStatus status = reply.status;

    // Make it compatible with the ascii protocol
    if (status == MemcacheStatus.KEY_EXISTS && opcode == OpCode.ADD) {
      status = MemcacheStatus.ITEM_NOT_STORED;
    } else if (status == MemcacheStatus.KEY_NOT_FOUND &&
            (opcode == OpCode.APPEND || opcode == OpCode.PREPEND)) {
      status = MemcacheStatus.ITEM_NOT_STORED;
    }

    succeed(status);
  }

  @Override
  public byte[] getValue() {
    return value;
  }
}

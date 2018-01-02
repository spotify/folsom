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

import com.google.common.primitives.Longs;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IncrRequest extends BinaryRequest<Long> {
  private final byte opcode;
  private final long by;
  private final long initial;
  private final int ttl;

  public IncrRequest(final byte[] key,
                     final byte opcode,
                     final long by,
                     final long initial,
                     final int ttl) {
    super(key);
    this.opcode = opcode;
    this.by = by;
    this.initial = initial;
    this.ttl = ttl;
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    final int expiration = Utils.ttlToExpiration(ttl);

    final int extraLength = 8 + 8 + 4; // by + initial + expiration

    writeHeader(dst, opcode, extraLength, 0, 0);
    dst.putLong(by);
    dst.putLong(initial);
    dst.putInt(expiration);
    dst.put(key);
    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(final BinaryResponse replies) throws IOException {
    final ResponsePacket reply = handleSingleReply(replies);

    if (reply.status == MemcacheStatus.OK) {
      succeed(Longs.fromByteArray(reply.value));
    } else if (reply.status == MemcacheStatus.KEY_NOT_FOUND) {
      succeed(null);
    } else {
      throw new IOException("Unexpected response: " + reply.status);
    }
  }
}

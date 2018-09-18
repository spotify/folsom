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

import com.google.common.collect.Lists;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.MultiRequest;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.client.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class MultigetRequest
        extends BinaryRequest<List<GetResult<byte[]>>>
        implements MultiRequest<GetResult<byte[]>> {

  private final int ttl;
  private final List<byte[]> keys;

  private MultigetRequest(final List<byte[]> keys,
                          final int ttl) {
    super(keys.get(0));
    this.keys = keys;
    this.ttl = ttl;
  }

  public static MultigetRequest create(final List<byte[]> keys, final int ttl) {
    final int size = keys.size();
    if (size > MemcacheEncoder.MAX_MULTIGET_SIZE) {
      throw new IllegalArgumentException("Too large multiget request");
    }
    return new MultigetRequest(keys, ttl);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    final int numKeys = keys.size();

    int expiration;
    int extrasLength;
    final boolean hasTTL = ttl > 0;
    if (hasTTL) {
      expiration = Utils.ttlToExpiration(ttl);
      extrasLength = 4;
    } else {
      expiration = 0;
      extrasLength = 0;
    }

    int multigetOpaque = opaque;
    int sequenceNumber = numKeys;
    for (final byte[] key : keys) {
      final int keyLength = key.length;
      final int totalLength = keyLength + extrasLength;

      final int opaque = multigetOpaque | --sequenceNumber;

      dst.put(MAGIC_NUMBER);
      dst.put(sequenceNumber == 0 ? OpCode.GET : OpCode.GETQ);
      dst.putShort((short) keyLength); // byte 2-3
      dst.put((byte) extrasLength); // byte 4
      dst.put((byte) 0); // byte 5-7, Data type, Reserved
      dst.put((byte) 0); // byte 5-7, Data type, Reserved
      dst.put((byte) 0); // byte 5-7, Data type, Reserved
      dst.putInt(totalLength); // byte 8-11
      dst.putInt(opaque); // byte 12-15, Opaque
      dst.putLong((long) 0); // byte 16-23, CAS
      if (hasTTL) {
        dst.putInt(expiration);
      }
      dst.put(key);
    }

    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(BinaryResponse replies) throws IOException {
    final int size = keys.size();

    final List<GetResult<byte[]>> result = Lists.newArrayListWithCapacity(size);
    for (int i = 0; i < size; i++) {
      result.add(null);
    }

    int expectedOpaque = opaque;

    for (final ResponsePacket reply : replies) {
      if (OpCode.getKind(reply.opcode) != OpCode.GET) {
        throw new IOException("Unmatched response");
      }
      final int opaque = reply.opaque & 0xFFFFFF00;
      if (opaque != expectedOpaque) {
        throw new IOException("messages out of order for " + getClass().getSimpleName());
      }
      final int sequenceCounter = reply.opaque & 0x000000FF;
      int index = size - sequenceCounter - 1;
      if (index < 0) {
        throw new IOException("Invalid index: " + index);
      }

      if (reply.status == MemcacheStatus.OK) {
        result.set(index, GetResult.success(reply.value, reply.cas));
      } else if (reply.status == MemcacheStatus.KEY_NOT_FOUND) {
        // No need to do anything
      } else {
        throw new IOException("Unexpected response: " + reply.status);
      }
    }
    succeed(result);
  }

  @Override
  public List<byte[]> getKeys() {
    return keys;
  }


  @Override
  public Request<List<GetResult<byte[]>>> create(List<byte[]> keys) {
    return new MultigetRequest(keys, ttl);
  }
}

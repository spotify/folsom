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
import java.nio.charset.Charset;
import java.util.List;

public class MultigetRequest
        extends BinaryRequest<List<GetResult<byte[]>>>
        implements MultiRequest<GetResult<byte[]>> {

  private final int expiration;
  private final byte opcode;
  private final byte opcodeQuiet;
  private final List<byte[]> keys;

  private MultigetRequest(final List<byte[]> keys,
                          final byte opcode,
                          final int expiration) {
    super(keys.get(0));
    this.keys = keys;
    if (opcode == OpCode.GET) {
      this.opcode = OpCode.GET;
      this.opcodeQuiet = OpCode.GETQ;
    } else if (opcode == OpCode.GAT) {
      this.opcode = OpCode.GAT;
      this.opcodeQuiet = OpCode.GATQ;
    } else {
      throw new IllegalArgumentException("Unsupported opcode: " + opcode);
    }
    this.expiration = expiration;
  }

  public static MultigetRequest create(final List<String> keys, Charset charset,
                                       final byte opcode,
                                       final int ttl) {
    final int size = keys.size();
    if (size > MemcacheEncoder.MAX_MULTIGET_SIZE) {
      throw new IllegalArgumentException("Too large multiget request");
    }
    final int expiration;
    if (opcode == OpCode.GAT) {
      expiration = Utils.ttlToExpiration(ttl);
    } else {
      expiration = 0;
    }
    return new MultigetRequest(encodeKeys(keys, charset), opcode, expiration);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    final int numKeys = keys.size();

    int extrasLength;
    final boolean hasTTL = expiration > 0;
    if (hasTTL) {
      extrasLength = 4;
    } else {
      extrasLength = 0;
    }

    int multigetOpaque = this.getOpaque();
    int sequenceNumber = numKeys;
    for (final byte[] key : keys) {
      final int keyLength = key.length;
      final int totalLength = keyLength + extrasLength;

      final int opaque = multigetOpaque | --sequenceNumber;

      dst.put(MAGIC_NUMBER);
      dst.put(sequenceNumber == 0 ? opcode : opcodeQuiet);
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

    int expectedOpaque = this.getOpaque();

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
  public Request<List<GetResult<byte[]>>> create(final List<byte[]> keys) {
    return new MultigetRequest(keys, opcode, expiration);
  }
}

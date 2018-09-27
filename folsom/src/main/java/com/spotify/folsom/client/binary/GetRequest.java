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

import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheAuthenticationException;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.client.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

public class GetRequest
        extends BinaryRequest<GetResult<byte[]>>
        implements com.spotify.folsom.client.GetRequest {
  private final byte opcode;
  private final int ttl;

  public GetRequest(final byte[] key,
                    final byte opcode,
                    final int ttl) {
    super(key);
    this.opcode = checkNotNull(opcode, "opcode");
    this.ttl = checkNotNull(ttl, "ttl");
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    int expiration;
    int extrasLength;
    if (ttl > 0) {
      expiration = Utils.ttlToExpiration(ttl);
      extrasLength = 4;
    } else {
      expiration = 0;
      extrasLength = 0;
    }

    writeHeader(dst, opcode, extrasLength, 0, 0);
    if (ttl > 0) {
      dst.putInt(expiration);
    }
    dst.put(key);
    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(final BinaryResponse replies) throws IOException {
    final ResponsePacket reply = handleSingleReply(replies);

    if (OpCode.getKind(reply.opcode) != OpCode.GET) {
      throw new IOException("Unmatched response");
    }
    if (reply.status == MemcacheStatus.OK) {
      succeed(GetResult.success(reply.value, reply.cas));
    } else if (reply.status == MemcacheStatus.KEY_NOT_FOUND) {
      succeed(null);
    } else if (reply.status == MemcacheStatus.UNAUTHORIZED) {
      fail(new MemcacheAuthenticationException("Authentication failed"));
    } else {
      throw new IOException("Unexpected response: " + reply.status);
    }
  }
}

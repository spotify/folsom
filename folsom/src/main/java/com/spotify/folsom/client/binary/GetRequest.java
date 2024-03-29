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

import static java.util.Objects.requireNonNull;

import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.client.Utils;
import com.spotify.folsom.guava.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;

public class GetRequest extends BinaryRequest<GetResult<byte[]>>
    implements com.spotify.folsom.client.GetRequest {
  private final OpCode opcode;
  private final int ttl;

  public GetRequest(final byte[] key, final OpCode opcode, final int ttl) {
    super(key);
    this.opcode = requireNonNull(opcode, "opcode");
    this.ttl = requireNonNull(ttl, "ttl");
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
  public Request<GetResult<byte[]>> duplicate() {
    return new GetRequest(key, opcode, ttl);
  }

  @Override
  public void handle(final BinaryResponse replies, final HostAndPort server) throws IOException {
    final ResponsePacket reply = handleSingleReply(replies);

    if (OpCode.getKind(reply.opcode) != OpCode.GET) {
      throw new IOException("Unmatched response");
    }
    if (reply.status == MemcacheStatus.OK) {
      succeed(GetResult.success(reply.value, reply.cas, reply.flags));
    } else if (reply.status == MemcacheStatus.KEY_NOT_FOUND) {
      succeed(null);
    } else {
      throw new IOException("Unexpected response: " + reply.status);
    }
  }
}

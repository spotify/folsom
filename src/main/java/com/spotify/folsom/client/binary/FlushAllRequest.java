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

package com.spotify.folsom.client.binary;

import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.FanoutRequest;
import com.spotify.folsom.client.OpCode;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class FlushAllRequest extends BinaryRequest<Void> implements FanoutRequest {
  private final int delay;

  public FlushAllRequest(final int delay, final int opaque) {
    super("", opaque);
    this.delay = delay;
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    writeBinaryHeader(dst, OpCode.FLUSH, 0, 4, 4, 0, opaque);
    dst.putInt(delay);
    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(final BinaryResponse replies) throws IOException {
    ResponsePacket reply = handleSingleReply(replies);

    if (reply.status == MemcacheStatus.OK) {
      succeed(null);
    } else {
      throw new IOException("Unexpected response: " + reply.status);
    }
  }
}

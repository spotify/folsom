/*
 * Copyright (c) 2018 Spotify AB
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

public class FlushRequest extends BinaryRequest<MemcacheStatus>
    implements com.spotify.folsom.client.FlushRequest {

  public static final byte[] NO_KEY = new byte[0];
  private final int delay;

  public FlushRequest(final int delay) {
    super(NO_KEY);
    this.delay = delay;
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    final int expiration = Utils.ttlToExpiration(delay);
    final int extrasLength = 4;

    writeHeader(dst, OpCode.FLUSH, extrasLength, 0, 0);
    dst.putInt(expiration);

    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(final BinaryResponse replies) throws IOException {
    ResponsePacket reply = handleSingleReply(replies);
    succeed(reply.status);
  }
}

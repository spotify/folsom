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
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.OpCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NoopRequest extends BinaryRequest<Void> {

  // Keys have to be valid, so pick the key "X" even though we will never actually use it.
  private static final byte[] DUMMY_KEY = "X".getBytes(Charsets.US_ASCII);

  public NoopRequest() {
    super(DUMMY_KEY);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    dst.put(MAGIC_NUMBER);
    dst.put(OpCode.NOOP);
    dst.putShort((short) 0); // byte 2-3
    dst.put((byte) 0); // byte 4
    dst.put((byte) 0);
    dst.put((byte) 0);
    dst.put((byte) 0);
    dst.putInt(0); // byte 8-11
    dst.putInt(getOpaque()); // byte 12-15, Opaque
    dst.putLong((long) 0); // byte 16-23, CAS
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

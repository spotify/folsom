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

package com.spotify.folsom.client.ascii;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class FlushAllRequest extends AsciiRequest<Void> {
  private static final byte[] CMD = "flush_all ".getBytes();
  private final int delay;

  public FlushAllRequest(int delay) {
    super("");
    this.delay = delay;
  }

  @Override
  public ByteBuf writeRequest(ByteBufAllocator alloc, ByteBuffer dst) {
    dst.put(CMD);
    dst.put(String.valueOf(delay).getBytes());
    dst.put(NEWLINE_BYTES);
    return toBuffer(alloc, dst);
  }

  @Override
  protected void handle(AsciiResponse response) throws IOException {
    AsciiResponse.Type type = response.type;
    if (type == AsciiResponse.Type.OK) {
      succeed(null);
    } else {
      throw new IOException("Unexpected line: " + type);
    }
  }
}

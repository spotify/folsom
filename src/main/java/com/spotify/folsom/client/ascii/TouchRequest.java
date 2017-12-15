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

import com.spotify.folsom.MemcacheStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TouchRequest extends AsciiRequest<MemcacheStatus> {
  private static final byte[] CMD = "touch ".getBytes();
  private final int ttl;

  public TouchRequest(byte[] key, int ttl) {
    super(key);
    this.ttl = ttl;
  }

  @Override
  public ByteBuf writeRequest(ByteBufAllocator alloc, ByteBuffer dst) {
    // <command name> <key> <value> [noreply]\r\n
    dst.put(CMD);
    dst.put(key);
    dst.put(SPACE_BYTES);
    dst.put(String.valueOf(ttl).getBytes());
    dst.put(NEWLINE_BYTES);
    return toBuffer(alloc, dst);
  }

  @Override
  protected void handle(AsciiResponse response) throws IOException {
    AsciiResponse.Type type = response.type;
    if (type == AsciiResponse.Type.TOUCHED) {
      succeed(MemcacheStatus.OK);
    } else if (type == AsciiResponse.Type.NOT_FOUND) {
      succeed(MemcacheStatus.KEY_NOT_FOUND);
    } else {
      throw new IOException("Unexpected line: " + type);
    }
  }
}

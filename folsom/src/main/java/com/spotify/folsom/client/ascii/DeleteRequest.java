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

package com.spotify.folsom.client.ascii;

import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.guava.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DeleteRequest extends AsciiRequest<MemcacheStatus> {

  private static final byte[] CMD_BYTES = "delete ".getBytes(StandardCharsets.US_ASCII);

  public DeleteRequest(final byte[] key) {
    super(key);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    dst.put(CMD_BYTES);
    dst.put(key);
    dst.put(NEWLINE_BYTES);
    return toBuffer(alloc, dst);
  }

  @Override
  public Request<MemcacheStatus> duplicate() {
    return new DeleteRequest(key);
  }

  @Override
  public void handle(final AsciiResponse response, final HostAndPort server) throws IOException {
    switch (response.type) {
      case DELETED:
        succeed(MemcacheStatus.OK);
        return;
      case NOT_FOUND:
        succeed(MemcacheStatus.KEY_NOT_FOUND);
        return;
      default:
        throw new IOException("Unexpected response type: " + response.type);
    }
  }
}

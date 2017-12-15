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

import com.google.common.base.Charsets;
import com.spotify.folsom.client.ascii.AsciiResponse.Type;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IncrRequest extends AsciiRequest<Long> {

  private static final byte[] INCR_CMD = "incr ".getBytes(Charsets.US_ASCII);
  private  static final byte[] DECR_CMD = "decr ".getBytes(Charsets.US_ASCII);

  private final byte[] operation;
  private final long by;

  private IncrRequest(final byte[] operation,
                      final byte[] key,
                      final long by) {
    super(key);
    this.operation = operation;
    this.by = by;
  }

  public static IncrRequest createIncr(final byte[] key, final long value) {
    return new IncrRequest(INCR_CMD, key, value);
  }

  public static IncrRequest createDecr(final byte[] key, final long value) {
    return new IncrRequest(DECR_CMD, key, value);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    // <command name> <key> <value> [noreply]\r\n
    dst.put(operation);
    dst.put(key);
    dst.put(SPACE_BYTES);
    dst.put(String.valueOf(by).getBytes());
    dst.put(NEWLINE_BYTES);
    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(final AsciiResponse response) throws IOException {
    if (response instanceof NumericAsciiResponse) {
      succeed(((NumericAsciiResponse) response).numericValue);
    } else if (response.type == Type.NOT_FOUND) {
      succeed(null);
    } else {
      throw new IOException("Unexpected response type: " + response.type);
    }
  }

  public long getBy() {
    return by;
  }

  public long multiplier() {
    return operation == INCR_CMD ? 1 : -1;
  }
}

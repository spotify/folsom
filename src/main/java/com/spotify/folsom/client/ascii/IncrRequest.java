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
import com.spotify.folsom.client.Utils;
import com.spotify.folsom.client.ascii.AsciiResponse.Type;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;

public class IncrRequest extends AsciiRequest<Long> {

  private static final EnumMap<Operation, byte[]> CMD;
  static {
    CMD = new EnumMap<>(Operation.class);
    CMD.put(Operation.INCR, "incr ".getBytes(Charsets.US_ASCII));
    CMD.put(Operation.DECR, "decr ".getBytes(Charsets.US_ASCII));
  }

  private final Operation operation;
  private final long value;

  private IncrRequest(final Operation operation,
                      final String key,
                      final long value) {
    super(key);
    this.operation = operation;
    this.value = value;
  }

  public static IncrRequest create(final Operation operation,
                                   final String key,
                                   final long value) {
    if (operation == null) {
      throw new IllegalArgumentException("Invalid operation: " + operation);
    }
    return new IncrRequest(operation, key, value);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    // <command name> <key> <value> [noreply]\r\n
    dst.put(CMD.get(operation));
    Utils.writeKeyString(dst, key);
    dst.put((byte) ' ');
    dst.put(String.valueOf(value).getBytes());
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

  public enum Operation {
    INCR, DECR
  }
}

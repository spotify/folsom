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
import com.spotify.folsom.MemcacheStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;

public class SetRequest
        extends AsciiRequest<MemcacheStatus>
        implements com.spotify.folsom.client.SetRequest {

  private static final EnumMap<Operation, byte[]> CMD;
  static {
    CMD = new EnumMap<>(Operation.class);
    CMD.put(Operation.SET, "set ".getBytes(Charsets.US_ASCII));
    CMD.put(Operation.ADD, "add ".getBytes(Charsets.US_ASCII));
    CMD.put(Operation.REPLACE, "replace ".getBytes(Charsets.US_ASCII));
    CMD.put(Operation.APPEND, "append ".getBytes(Charsets.US_ASCII));
    CMD.put(Operation.PREPEND, "prepend ".getBytes(Charsets.US_ASCII));
    CMD.put(Operation.CAS, "cas ".getBytes(Charsets.US_ASCII));
  }

  private static final byte[] FLAGS = " 0 ".getBytes(Charsets.US_ASCII);

  private final Operation operation;
  private final byte[] value;
  private final int ttl;
  private final long cas;

  private SetRequest(final Operation operation,
                     final byte[] key,
                     final byte[] value, final int ttl, final long cas) {
    super(key);
    this.operation = operation;
    this.value = value;
    this.ttl = ttl;
    this.cas = cas;
  }

  public static SetRequest casSet(final byte[] key, final byte[] value,
                                  final int ttl, final long cas) {
    return new SetRequest(Operation.CAS, key, value, ttl, cas);
  }

  public static SetRequest create(final Operation operation,
                                  final byte[] key,
                                  final byte[] value,
                                  final int ttl) {
    if (operation == null || operation == Operation.CAS) {
      throw new IllegalArgumentException("Invalid operation: " + operation);
    }
    return new SetRequest(operation, key, value, ttl, 0);
  }

  @Override
  public ByteBuf writeRequest(ByteBufAllocator alloc, ByteBuffer dst) {
    // <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
    // "cas" <key> <flags> <exptime> <cas unique> <bytes> [noreply]\r\n
    dst.put(CMD.get(operation));
    dst.put(key);
    dst.put(FLAGS);
    dst.put(String.valueOf(ttl).getBytes());
    dst.put(SPACE_BYTES);
    dst.put(String.valueOf(value.length).getBytes());
    if (operation == Operation.CAS) {
      dst.put(SPACE_BYTES);
      dst.put(String.valueOf(cas).getBytes());
    }
    dst.put(NEWLINE_BYTES);

    if (dst.remaining() >= value.length + NEWLINE_BYTES.length) {
      dst.put(value);
      dst.put(NEWLINE_BYTES);
      return toBuffer(alloc, dst);
    } else {
      return toBufferWithValueAndNewLine(alloc, dst, value);
    }
  }

  private static ByteBuf toBufferWithValueAndNewLine(final ByteBufAllocator alloc, ByteBuffer dst,
                                                     byte[] value) {
    ByteBuf buffer = toBuffer(alloc, dst, value.length + NEWLINE_BYTES.length);
    buffer.writeBytes(value);
    buffer.writeBytes(NEWLINE_BYTES);
    return buffer;
  }

  @Override
  public void handle(AsciiResponse response) throws IOException {
    switch (response.type) {
      case STORED:
        succeed(MemcacheStatus.OK);
        return;
      case NOT_STORED:
        succeed(MemcacheStatus.ITEM_NOT_STORED);
        return;
      case EXISTS:
        succeed(MemcacheStatus.KEY_EXISTS);
        return;
      case NOT_FOUND:
        if (operation == Operation.APPEND || operation == Operation.PREPEND) {
          succeed(MemcacheStatus.ITEM_NOT_STORED);
        } else {
          succeed(MemcacheStatus.KEY_NOT_FOUND);
        }
        return;
      default:
        throw new IOException("Unexpected line: " + response.type);
    }
  }

  public enum Operation {
    SET, ADD, REPLACE, APPEND, PREPEND, CAS
  }

  @Override
  public byte[] getValue() {
    return value;
  }
}

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

import static com.spotify.folsom.MemcacheStatus.OK;
import static com.spotify.folsom.MemcacheStatus.UNAUTHORIZED;
import static com.spotify.folsom.MemcacheStatus.UNKNOWN_COMMAND;
import static java.util.Objects.requireNonNull;

import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.guava.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class PlaintextAuthenticateRequest extends BinaryRequest<MemcacheStatus> {

  private static final byte[] KEY = "PLAIN".getBytes(StandardCharsets.US_ASCII);

  private final String username;
  private final String password;

  public PlaintextAuthenticateRequest(final String username, final String password) {
    // Key is auth type
    super(KEY);

    this.username = requireNonNull(username);
    this.password = requireNonNull(password);
  }

  @Override
  public ByteBuf writeRequest(ByteBufAllocator alloc, ByteBuffer dst) {
    final int valueLength = 2 + username.length() + password.length();

    writeHeader(dst, OpCode.SASL_AUTH, (short) 0, valueLength, 0L);

    // Write "PLAIN" -> auth type
    dst.put(key);

    final byte separator = 0x00;
    dst.put(separator);
    dst.put(username.getBytes(StandardCharsets.US_ASCII));
    dst.put(separator);
    dst.put(password.getBytes(StandardCharsets.US_ASCII));

    return toBuffer(alloc, dst);
  }

  @Override
  protected void handle(final BinaryResponse replies, final HostAndPort server) throws IOException {
    ResponsePacket reply = handleSingleReply(replies);

    if (OpCode.getKind(reply.opcode) != OpCode.SASL_AUTH) {
      throw new IOException("Unmatched response");
    }

    MemcacheStatus status = reply.status;
    if (status == UNKNOWN_COMMAND) {
      // Unknown command implies no authorization needed.
      succeed(OK);
    } else if (status == OK || status == UNAUTHORIZED) {
      succeed(status);
    } else {
      final IOException exception =
          new IOException(
              String.format("Invalid status %s, expected OK or UNAUTHORIZED.", status.toString()));
      fail(exception, server);
    }
  }
}

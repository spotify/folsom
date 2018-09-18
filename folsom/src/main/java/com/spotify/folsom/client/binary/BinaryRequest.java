/*
 * Copyright (c) 2014-2018 Spotify AB
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

import com.spotify.folsom.client.Request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

public abstract class BinaryRequest<V> extends Request<V> {

  protected static final int HEADER_SIZE = 24;
  protected static final byte MAGIC_NUMBER = (byte) 0x80;

  protected final int opaque;

  protected BinaryRequest(final byte[] key) {
    super(key);
    opaque = (ThreadLocalRandom.current().nextInt() << 8) & 0xFFFFFF00;
  }

  public void writeHeader(final ByteBuffer dst, final byte opCode,
                          final int extraLength, final int valueLength,
                          final long cas) {
    int keyLength = key.length;

    dst.put(MAGIC_NUMBER);
    dst.put(opCode);
    dst.putShort((short) keyLength); // byte 2-3
    dst.put((byte) extraLength); // byte 4
    dst.put((byte) 0);
    dst.put((byte) 0);
    dst.put((byte) 0);
    dst.putInt(extraLength + keyLength + valueLength); // byte 8-11
    dst.putInt(opaque); // byte 12-15, Opaque
    dst.putLong(cas); // byte 16-23, CAS
  }

  protected ResponsePacket handleSingleReply(BinaryResponse replies) throws IOException {
    if (replies.size() != 1) {
      throw new IOException(
              "got " + replies.size() + " replies but expected 1 for " +
              getClass().getSimpleName());
    }

    final ResponsePacket reply = replies.get(0);
    if (reply.opaque != opaque) {
      throw new IOException("messages out of order for " + getClass().getSimpleName());
    }
    return reply;
  }

  @Override
  public void handle(Object response) throws IOException {
    handle((BinaryResponse) response);
  }

  protected abstract void handle(BinaryResponse response) throws IOException;
}

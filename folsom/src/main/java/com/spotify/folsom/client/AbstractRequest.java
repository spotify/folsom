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
package com.spotify.folsom.client;

import static java.util.Objects.requireNonNull;

import com.spotify.folsom.guava.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public abstract class AbstractRequest<V> extends CompletableFuture<V> implements Request<V> {
  protected final byte[] key;

  protected AbstractRequest(byte[] key) {
    this.key = key;
  }

  protected static ByteBuf toBuffer(final ByteBufAllocator alloc, ByteBuffer dst) {
    return toBuffer(alloc, dst, 0);
  }

  @Override
  public byte[] getKey() {
    return key;
  }

  @Override
  public void fail(final Throwable e, final HostAndPort address) {
    completeExceptionally(e);
  }

  @Override
  public CompletionStage<V> asFuture() {
    return this;
  }

  public void succeed(final V result) {
    complete(result);
  }

  protected static ByteBuf toBuffer(final ByteBufAllocator alloc, ByteBuffer dst, int extra) {
    // TODO (dano): write directly to target buffer
    dst.flip();
    final ByteBuf buffer = alloc.buffer(dst.remaining() + extra);
    buffer.writeBytes(dst);
    return buffer;
  }

  public static byte[] encodeKey(String key, Charset charset, int maxKeyLength) {
    requireNonNull(key, "key");
    requireNonNull(charset, "charset");
    byte[] keyBytes = key.getBytes(charset);
    int length = keyBytes.length;
    if (length > maxKeyLength) {
      String message =
          "Key is too long. Max-length is " + maxKeyLength + " but key was " + length + ": " + key;
      throw new IllegalArgumentException(message);
    }
    if (length <= 0) {
      throw new IllegalArgumentException("Key is empty");
    }
    for (int i = 0; i < length; i++) {
      final byte c = keyBytes[i];
      if (c >= 0 && c <= 32) {
        throw new IllegalArgumentException("Invalid key: " + key);
      }
    }
    return keyBytes;
  }

  public static List<byte[]> encodeKeys(List<String> keys, Charset charset, int maxKeyLength) {
    List<byte[]> res = new ArrayList<>(keys.size());
    for (String key : keys) {
      res.add(encodeKey(key, charset, maxKeyLength));
    }
    return res;
  }
}

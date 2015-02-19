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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Request<V> extends AbstractFuture<V> {
  protected final byte[] key;

  protected Request(String key, Charset charset) {
    this(encodeKey(key, charset));
  }

  protected Request(byte[] key) {
    this.key = key;
  }

  protected static ByteBuf toBuffer(final ByteBufAllocator alloc, ByteBuffer dst) {
    return toBuffer(alloc, dst, 0);
  }

  public byte[] getKey() {
    return key;
  }

  public abstract ByteBuf writeRequest(final ByteBufAllocator alloc, ByteBuffer dst);

  public void fail(final Throwable e) {
    setException(e);
  }

  public void succeed(final V result) {
    set(result);
  }

  protected static ByteBuf toBuffer(final ByteBufAllocator alloc, ByteBuffer dst, int extra) {
    // TODO (dano): write directly to target buffer
    dst.flip();
    final ByteBuf buffer = alloc.buffer(dst.remaining() + extra);
    buffer.writeBytes(dst);
    return buffer;
  }

  public abstract void handle(Object response) throws IOException;

  @VisibleForTesting
  protected static byte[] encodeKey(String key, Charset charset) {
    checkNotNull(key, "key");
    checkNotNull(charset, "charset");
    byte[] keyBytes = key.getBytes(charset);
    int length = keyBytes.length;
    if (length > 250) {
      throw new IllegalArgumentException("Key is too long: " + key);
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

  protected static List<byte[]> encodeKeys(List<String> keys, Charset charset) {
    List<byte[]> res = Lists.newArrayListWithCapacity(keys.size());
    for (String key : keys) {
      res.add(encodeKey(key, charset));
    }
    return res;
  }
}

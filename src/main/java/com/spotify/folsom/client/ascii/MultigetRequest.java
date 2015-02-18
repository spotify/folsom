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
import com.google.common.collect.Lists;

import com.spotify.folsom.GetResult;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.client.ShardedRequest;
import com.spotify.folsom.client.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class MultigetRequest
        extends AsciiRequest<List<GetResult<byte[]>>>
        implements ShardedRequest<GetResult<byte[]>> {

  private static final byte[] GET = "get ".getBytes(Charsets.US_ASCII);
  private static final byte[] CAS_GET = "gets ".getBytes(Charsets.US_ASCII);

  private final List<String> keys;
  private final byte[] cmd;

  private MultigetRequest(final List<String> keys, byte[] cmd) {
    super(keys.get(0));
    this.cmd = cmd;
    this.keys = keys;
  }

  public static MultigetRequest create(final List<String> keys, boolean withCas) {
    byte[] cmd = withCas ? CAS_GET : GET;
    for (int i = 1; i < keys.size(); i++) {
      Utils.validateKey(keys.get(i));
    }
    final int size = keys.size();
    if (size >= 256) {
      throw new IllegalArgumentException("Too large multiget request");
    }
    return new MultigetRequest(keys, cmd);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    dst.put(cmd);
    for (final String key : keys) {
      dst.put(SPACE_BYTES);
      Utils.writeKeyString(dst, key);
    }
    dst.put(NEWLINE_BYTES);

    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(AsciiResponse response) throws IOException {
    if (!(response instanceof ValueAsciiResponse)) {
      throw new IOException("Unexpected response type: " + response.type);
    }

    List<ValueResponse> values = ((ValueAsciiResponse) response).values;

    final int size = keys.size();

    if (values.size() > size) {
      throw new IOException("Too many responses, expected " + size + " but got " + values.size());
    }

    final List<GetResult<byte[]>> result = Lists.newArrayListWithCapacity(size);
    for (int i = 0; i < size; i++) {
      result.add(null);
    }
    int index = -1;
    for (final ValueResponse value : values) {
      index = findKey(index + 1, value.key);
      if (index < 0) {
        throw new IOException("Got key in value that was not present in request");
      }
      result.set(index, GetResult.success(value.value, value.cas));
    }
    succeed(result);
  }

  private int findKey(int index, final String key) {
    final int size = keys.size();
    while (index < size) {
      final String candidate = keys.get(index);
      if (candidate.equals(key)) {
        return index;
      }
      index++;
    }
    return -1;
  }

  @Override
  public List<String> getKeys() {
    return keys;
  }

  @Override
  public Request<List<GetResult<byte[]>>> create(List<String> keys) {
    return new MultigetRequest(keys, cmd);
  }
}

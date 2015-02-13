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
import com.spotify.folsom.GetResult;
import com.spotify.folsom.client.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class GetRequest
        extends AsciiRequest<GetResult<byte[]>>
        implements com.spotify.folsom.client.GetRequest {

  private static final byte[] GET = "get ".getBytes(Charsets.US_ASCII);
  private static final byte[] CAS_GET = "gets ".getBytes(Charsets.US_ASCII);

  private final byte[] cmd;

  public GetRequest(final String key, boolean withCas) {
    super(key);
    this.cmd = withCas ? CAS_GET : GET;
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    dst.put(cmd);
    Utils.writeKeyString(dst, key);
    dst.put(NEWLINE_BYTES);
    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(final AsciiResponse response) throws IOException {
    if (!(response instanceof ValueAsciiResponse)) {
      throw new IOException("Unexpected response type: " + response.type);
    }

    List<ValueResponse> values = ((ValueAsciiResponse) response).values;

    if (values.isEmpty()) {
      succeed(null);
      return;
    }

    if (values.size() > 1) {
      throw new IOException("Too many responses, expected 1 but got " + values.size());
    }

    ValueResponse valueResponse = values.get(0);
    if (!valueResponse.key.equals(key)) {
      throw new IOException("Expected key " + key + " but got " + valueResponse.key);
    }

    succeed(GetResult.success(valueResponse.value, valueResponse.cas));
  }
}

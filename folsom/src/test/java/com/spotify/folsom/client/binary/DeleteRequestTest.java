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

package com.spotify.folsom.client.binary;

import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.OpCode;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class DeleteRequestTest extends RequestTestTemplate {
  private static final String KEY = "foo";

  @Test
  public void testBuffer() throws Exception {
    DeleteRequest req = new DeleteRequest(KEY.getBytes(StandardCharsets.UTF_8), 0L);
    ByteBuf b = encode(req);

    assertHeader(b, OpCode.DELETE, KEY.length(), 0, KEY.length(), req.opaque, 0);
    assertString(KEY, b);
    assertEOM(b);
  }

  @Test
  public void testBufferWithCas() throws Exception {
    DeleteRequest req = new DeleteRequest(KEY.getBytes(StandardCharsets.UTF_8), 123L);
    ByteBuf b = encode(req);

    assertHeader(b, OpCode.DELETE, KEY.length(), 0, KEY.length(), req.opaque, 123);
    assertString(KEY, b);
    assertEOM(b);
  }

  private ByteBuf encode(final DeleteRequest req) throws Exception {
    MemcacheEncoder memcacheEncoder = new MemcacheEncoder();
    List<Object> out = new ArrayList<>();
    memcacheEncoder.encode(ctx, req, out);
    return (ByteBuf) out.get(0);
  }
}

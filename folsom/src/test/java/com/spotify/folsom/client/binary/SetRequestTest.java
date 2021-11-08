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

import static org.junit.Assert.assertEquals;

import com.spotify.folsom.client.Flags;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.OpCode;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class SetRequestTest extends RequestTestTemplate {
  private static final String KEY = "foo";
  private static final String VALUE = "val";

  @Test
  public void testBufferNoCas() throws Exception {
    verifySetRequest(0);
  }

  @Test
  public void testBufferCas() throws Exception {
    verifySetRequest(258);
  }

  @Test
  public void testZeroTTL() throws Exception {
    verifySetRequest(0, 258);
  }

  @Test
  public void testFlags() throws Exception {
    verifySetRequest(1000, 258, new Flags(42));
  }

  private void verifySetRequest(long cas) throws Exception {
    verifySetRequest(1000, cas);
  }

  private void verifySetRequest(int ttl, long cas) throws Exception {
    verifySetRequest(ttl, cas, Flags.DEFAULT);
  }

  private void verifySetRequest(int ttl, long cas, Flags flags) throws Exception {
    SetRequest req =
        new SetRequest(
            OpCode.SET, KEY.getBytes(StandardCharsets.UTF_8), TRANSCODER.encode(VALUE), ttl, cas, flags);

    MemcacheEncoder memcacheEncoder = new MemcacheEncoder();
    List<Object> out = new ArrayList<>();
    memcacheEncoder.encode(ctx, req, out);
    ByteBuf b = (ByteBuf) out.get(0);

    final int keyLen = KEY.length();
    assertHeader(b, OpCode.SET, keyLen, 8, keyLen + 8 + VALUE.length(), req.opaque, cas);
    assertEquals(flags.asInt(), b.readInt());
    assertEquals(ttl, b.readInt());
    assertString(KEY, b);
    assertString(VALUE, b);
    assertEOM(b);
  }
}

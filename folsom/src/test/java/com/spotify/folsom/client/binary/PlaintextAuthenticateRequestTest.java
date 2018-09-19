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

import com.google.common.collect.Lists;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.OpCode;
import io.netty.buffer.ByteBuf;
import java.util.List;
import org.junit.Test;

public class PlaintextAuthenticateRequestTest extends RequestTestTemplate {
  private static final String KEY = "PLAIN";

  private static final String USERNAME = "memcached";
  private static final String PASSWORD = "foobar";
  private static final String VALUE = '\u0000' + USERNAME + '\u0000' + PASSWORD;

  @Test
  public void testBuffer() throws Exception {
    PlaintextAuthenticateRequest req = new PlaintextAuthenticateRequest(USERNAME, PASSWORD);

    MemcacheEncoder memcacheEncoder = new MemcacheEncoder();
    List<Object> out = Lists.newArrayList();
    memcacheEncoder.encode(ctx, req, out);
    ByteBuf b = (ByteBuf) out.get(0);

    assertHeader(b, OpCode.SASL_AUTH, KEY.length(), 0, KEY.length() + VALUE.length(), req.opaque, 0);
    assertString(KEY, b);
    assertString(VALUE, b);
    assertEOM(b);
  }
}

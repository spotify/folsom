/*
 * Copyright (c) 2019 Spotify AB
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
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class StatsRequestTest extends RequestTestTemplate {
  private static final String KEY = "slabs";

  @Test
  public void testRequest() throws Exception {
    StatsRequest request = new StatsRequest(KEY);
    MemcacheEncoder memcacheEncoder = new MemcacheEncoder();
    List<Object> out = new ArrayList<>();
    memcacheEncoder.encode(ctx, request, out);
    ByteBuf b = (ByteBuf) out.get(0);

    assertHeader(b, OpCode.STAT, KEY.length(), 0, KEY.length(), request.opaque, 0);
    assertString(KEY, b);
    assertEOM(b);
  }
}

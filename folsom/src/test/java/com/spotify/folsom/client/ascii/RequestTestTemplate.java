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

package com.spotify.folsom.client.ascii;

import static org.junit.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.nio.ByteBuffer;


public class RequestTestTemplate {

  protected void assertRequest(AsciiRequest<?> request, String expectedCmd) {
    ByteBufAllocator bba = UnpooledByteBufAllocator.DEFAULT;
    ByteBuffer bb = ByteBuffer.allocate(1024);

    ByteBuf out = request.writeRequest(bba, bb);

    byte[] b = new byte[out.readableBytes()];
    out.readBytes(b);
    assertEquals(expectedCmd, new String(b));
  }
}

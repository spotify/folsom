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

import com.spotify.folsom.client.ascii.IncrRequest.Operation;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import static org.junit.Assert.assertEquals;


public class IncrRequestTest {

  @Test
  public void testIncrRequest() throws Exception {
    assertRequest(Operation.INCR, "incr");
  }

  @Test
  public void testDecrRequest() throws Exception {
    assertRequest(Operation.DECR, "decr");
  }

  private void assertRequest(Operation operation, String expectedCmd) {
    IncrRequest req = IncrRequest.create(operation, "foo", 2);

    ByteBufAllocator bba = UnpooledByteBufAllocator.DEFAULT;
    ByteBuffer bb = ByteBuffer.allocate(100);
    ByteBuf out = req.writeRequest(bba, bb);

    byte[] b = new byte[out.readableBytes()];
    out.readBytes(b);
    assertEquals(expectedCmd + " foo 2\r\n", new String(b));
  }

  @Test
  public void testResponse() throws IOException, InterruptedException, ExecutionException {
    IncrRequest req = IncrRequest.create(Operation.INCR, "foo", 2);

    AsciiResponse response = new NumericAsciiResponse(123);
    req.handle(response);

    assertEquals(new Long(123), req.get());
  }

  @Test
  public void testNonFoundResponse() throws IOException, InterruptedException, ExecutionException {
    IncrRequest req = IncrRequest.create(Operation.INCR, "foo", 2);

    req.handle(AsciiResponse.NOT_FOUND);
    assertEquals(null, req.get());
  }
}

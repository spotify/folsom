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

import static org.junit.Assert.assertEquals;

import com.spotify.folsom.guava.HostAndPort;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class IncrRequestTest extends RequestTestTemplate {

  @Test
  public void testIncrRequest() {
    IncrRequest req = IncrRequest.createIncr("foo".getBytes(StandardCharsets.UTF_8), 2);
    assertRequest(req, "incr" + " foo 2\r\n");
  }

  @Test
  public void testDecrRequest() {
    IncrRequest req = IncrRequest.createDecr("foo".getBytes(StandardCharsets.UTF_8), 2);
    assertRequest(req, "decr" + " foo 2\r\n");
  }

  @Test
  public void testResponse() throws IOException, InterruptedException, ExecutionException {
    IncrRequest req = IncrRequest.createIncr("foo".getBytes(StandardCharsets.UTF_8), 2);

    AsciiResponse response = new NumericAsciiResponse(123);
    req.handle(response, HostAndPort.fromHost("host"));

    assertEquals(Long.valueOf(123), req.get());
  }

  @Test
  public void testNonFoundResponse() throws IOException, InterruptedException, ExecutionException {
    IncrRequest req = IncrRequest.createIncr("foo".getBytes(StandardCharsets.UTF_8), 2);

    req.handle(AsciiResponse.NOT_FOUND, HostAndPort.fromHost("host"));
    assertEquals(null, req.get());
  }
}

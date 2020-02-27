/*
 * Copyright (c) 2020 Spotify AB
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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;

import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.guava.HostAndPort;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class DeleteWithCasRequestTest extends RequestTestTemplate {

  private DeleteWithCasRequest req = new DeleteWithCasRequest("foo".getBytes(US_ASCII), 7);

  @Test
  public void testRequest() {
    assertRequest(req, "cas foo 0 -1 1 7\r\na\r\n");
  }

  @Test
  public void testResponse() throws IOException, InterruptedException, ExecutionException {
    req.handle(AsciiResponse.STORED, HostAndPort.fromHost("host"));
    assertEquals(MemcacheStatus.OK, req.get());
  }

  @Test
  public void testNotFoundResponse() throws IOException, InterruptedException, ExecutionException {
    req.handle(AsciiResponse.NOT_FOUND, HostAndPort.fromHost("host"));
    assertEquals(MemcacheStatus.KEY_NOT_FOUND, req.get());
  }

  @Test
  public void testKeyExistsResponse() throws IOException, InterruptedException, ExecutionException {
    req.handle(AsciiResponse.EXISTS, HostAndPort.fromHost("host"));
    assertEquals(MemcacheStatus.KEY_EXISTS, req.get());
  }
}

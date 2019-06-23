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

package com.spotify.folsom.client.ascii;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.spotify.folsom.MemcachedStats;
import com.spotify.folsom.guava.HostAndPort;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class StatsRequestTest extends RequestTestTemplate {

  private StatsRequest req = new StatsRequest("slabs");

  @Test
  public void testRequest() throws Exception {
    assertRequest(req, "stats slabs\r\n");
  }

  @Test
  public void testResponse() throws IOException, InterruptedException, ExecutionException {
    req.handle(new StatsAsciiResponse(), HostAndPort.fromParts("host", 123));

    assertEquals(ImmutableMap.of("host:123", new MemcachedStats(ImmutableMap.of())), req.get());
  }
}

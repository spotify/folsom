/*
 * Copyright (c) 2014-2019 Spotify AB
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

package com.spotify.folsom.elasticache;

import static com.spotify.folsom.guava.HostAndPort.fromParts;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

public class ResponseParserTest {

  private final ResponseParser parser = new ResponseParser();

  @Test
  public void parseFull() throws IOException {
    final InputStream in =
        in("CONFIG cluster 0 140\n2\nhost1|172.31.15.57|11211 host2|172.31.8.107|11212\n\r\nEND");

    final Response response = parser.parse(in);
    assertEquals(2, response.getConfigurationVersion());
    assertEquals(asList(fromParts("host1", 11211), fromParts("host2", 11212)), response.getHosts());
  }

  @Test
  public void parseSingleNode() throws IOException {
    final InputStream in = in("CONFIG cluster 0 140\n2\nhost1|172.31.15.57|11211\n\r\nEND");

    final Response response = parser.parse(in);
    assertEquals(2, response.getConfigurationVersion());
    assertEquals(asList(fromParts("host1", 11211)), response.getHosts());
  }

  @Test
  public void parseNoIp() throws IOException {
    final InputStream in =
        in("CONFIG cluster 0 140\n2\nhost1||11211 host2|172.31.8.107|11212\n\r\nEND");

    final Response response = parser.parse(in);
    assertEquals(2, response.getConfigurationVersion());
    assertEquals(asList(fromParts("host1", 11211), fromParts("host2", 11212)), response.getHosts());
  }

  @Test(expected = IOException.class)
  public void parseMissingEnd() throws IOException {
    final InputStream in = in("CONFIG cluster 0 140\n2\nhost1|172.31.15.57|11211\n\r\n");

    parser.parse(in);
  }

  @Test(expected = IOException.class)
  public void parseMissingEmpty() throws IOException {
    final InputStream in = in("CONFIG cluster 0 140\n2\nhost1|172.31.15.57|11211\n");

    parser.parse(in);
  }

  @Test(expected = IOException.class)
  public void parseError() throws IOException {
    final InputStream in = in("END");

    parser.parse(in);
  }

  private ByteArrayInputStream in(final String response) {
    return new ByteArrayInputStream(response.getBytes(US_ASCII));
  }
}

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

import static java.nio.charset.StandardCharsets.US_ASCII;

import com.google.common.base.Splitter;
import com.spotify.folsom.guava.HostAndPort;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ResponseParser {

  public Response parse(final InputStream in) throws IOException {
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in, US_ASCII))) {
      final String response = reader.readLine().trim();

      if (response.startsWith("CONFIG cluster ")) {
        final int configVersion = Integer.valueOf(reader.readLine()); // configuration version

        final List<String> hosts = Splitter.on(' ').splitToList(reader.readLine());

        final List<HostAndPort> result = new ArrayList<>();
        for (final String host : hosts) {
          final List<String> tokens = Splitter.on('|').splitToList(host);
          if (tokens.size() != 3) {
            throw new IOException("Expected 3 parts for a host, but got " + host);
          }

          // the private IP is not guaranteed to be included, so use the CNAME
          result.add(HostAndPort.fromParts(tokens.get(0), Integer.valueOf(tokens.get(2))));
        }

        // validate complete response
        final String emptyLine = reader.readLine();
        if (!"".equals(emptyLine)) {
          throw new IOException("Expected empty trailing line, invalid server response");
        }
        final String end = reader.readLine();
        if (!"END".equals(end)) {
          throw new IOException("Expected end of response, invalid server response");
        }

        return new Response(configVersion, result);
      } else {
        throw new IOException("Unexpected server response: " + response);
      }
    }
  }
}

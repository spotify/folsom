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

import com.google.common.collect.ImmutableMap;
import com.spotify.folsom.MemcachedStats;
import com.spotify.folsom.client.AllRequest;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.guava.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class StatsRequest extends AsciiRequest<Map<String, MemcachedStats>>
    implements AllRequest<Map<String, MemcachedStats>> {

  private static final byte[] CMD = "stats ".getBytes();

  public StatsRequest(final String key) {
    this(key.getBytes(StandardCharsets.US_ASCII));
  }

  private StatsRequest(final byte[] key) {
    super(key);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    dst.put(CMD);
    dst.put(key);
    dst.put(NEWLINE_BYTES);
    return toBuffer(alloc, dst);
  }

  @Override
  protected void handle(final AsciiResponse response, final HostAndPort server) throws IOException {
    final AsciiResponse.Type type = response.type;
    final String host = server.getHostText() + ":" + server.getPort();
    if (type == AsciiResponse.Type.STATS) {
      final StatsAsciiResponse statsResponse = (StatsAsciiResponse) response;
      succeed(ImmutableMap.of(host, new MemcachedStats(statsResponse.values)));
    } else if (type == AsciiResponse.Type.ERROR) {
      succeed(ImmutableMap.of(host, new MemcachedStats(ImmutableMap.of())));
    } else {
      throw new IOException("Unexpected response type: " + type);
    }
  }

  @Override
  public Map<String, MemcachedStats> merge(final List<Map<String, MemcachedStats>> results) {
    return AllRequest.mergeStats(results);
  }

  @Override
  public Request<Map<String, MemcachedStats>> duplicate() {
    return new StatsRequest(key);
  }
}

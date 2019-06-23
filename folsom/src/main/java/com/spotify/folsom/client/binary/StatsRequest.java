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

import com.google.common.collect.ImmutableMap;
import com.spotify.folsom.MemcachedStats;
import com.spotify.folsom.client.AllRequest;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.guava.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatsRequest extends BinaryRequest<Map<String, MemcachedStats>>
    implements AllRequest<Map<String, MemcachedStats>> {

  public StatsRequest(String key) {
    super(key.getBytes(StandardCharsets.US_ASCII));
  }

  private StatsRequest(final byte[] key) {
    super(key);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    writeHeader(dst, OpCode.STAT, 0, 0, 0);
    dst.put(key);

    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(final BinaryResponse replies, final HostAndPort server) throws IOException {
    final Map<String, String> stats = new HashMap<>();
    final int expectedOpaque = opaque;
    for (final ResponsePacket reply : replies) {
      if (OpCode.getKind(reply.opcode) != OpCode.STAT) {
        throw new IOException("Unmatched response");
      }
      final int opaque = reply.opaque;
      if (opaque != expectedOpaque) {
        throw new IOException("messages out of order for " + getClass().getSimpleName());
      }
      for (ResponsePacket responsePacket : replies) {
        final String name = new String(responsePacket.key, StandardCharsets.US_ASCII);
        final String value = new String(responsePacket.value, StandardCharsets.US_ASCII);
        stats.put(name, value);
      }
    }
    succeed(
        ImmutableMap.of(server.getHostText() + ":" + server.getPort(), new MemcachedStats(stats)));
  }

  @Override
  public Map<String, MemcachedStats> merge(List<Map<String, MemcachedStats>> results) {
    return AllRequest.mergeStats(results);
  }

  @Override
  public Request<Map<String, MemcachedStats>> duplicate() {
    return new StatsRequest(key);
  }
}

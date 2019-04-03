package com.spotify.folsom.client.ascii;

import com.google.common.collect.ImmutableMap;
import com.spotify.folsom.MemcachedStats;
import com.spotify.folsom.client.AllRequest;
import com.spotify.folsom.guava.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class StatsRequest extends AsciiRequest<Map<String, MemcachedStats>>
    implements AllRequest<Map<String, MemcachedStats>> {

  private static final byte[] CMD = "stats".getBytes();
  private static final byte[] NO_KEY = new byte[0];

  public StatsRequest() {
    super(NO_KEY);
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    dst.put(CMD);
    dst.put(NEWLINE_BYTES);
    return toBuffer(alloc, dst);
  }

  @Override
  protected void handle(final AsciiResponse response, final HostAndPort server) throws IOException {
    final AsciiResponse.Type type = response.type;
    if (type == AsciiResponse.Type.STATS) {
      final StatsAsciiResponse statsResponse = (StatsAsciiResponse) response;
      succeed(
          ImmutableMap.of(
              server.getHostText() + ":" + server.getPort(),
              new MemcachedStats(statsResponse.values)));
    } else {
      throw new IOException("Unexpected line: " + type);
    }
  }

  @Override
  public Map<String, MemcachedStats> merge(final List<Map<String, MemcachedStats>> results) {
    return AllRequest.mergeStats(results);
  }
}

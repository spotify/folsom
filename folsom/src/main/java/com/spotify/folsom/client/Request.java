package com.spotify.folsom.client;

import com.spotify.folsom.guava.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

public interface Request<V> {
  byte[] getKey();

  void fail(Throwable e, HostAndPort address);

  CompletionStage<V> asFuture();

  ByteBuf writeRequest(ByteBufAllocator alloc, ByteBuffer workingBuffer);

  void handle(Object msg, HostAndPort address) throws IOException;
}

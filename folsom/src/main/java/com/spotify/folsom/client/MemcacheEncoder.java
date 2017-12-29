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
package com.spotify.folsom.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.ByteBuffer;
import java.util.List;

public class MemcacheEncoder extends MessageToMessageEncoder<Request<?>> {

  public static final int MAX_KEY_LEN = 250;
  public static final int MAX_MULTIGET_SIZE = 255;
  public static final int MAX_ASCII_REQUEST = 100 + MAX_MULTIGET_SIZE * (MAX_KEY_LEN + 1);

  public static final int MAX_BINARY_REQUEST = MAX_MULTIGET_SIZE * (MAX_KEY_LEN + 32);
  public static final int MAX_REQUEST = Math.max(MAX_ASCII_REQUEST, MAX_BINARY_REQUEST);

  // Big enough to encode the largest possible request
  private final ByteBuffer workingBuffer = ByteBuffer.allocate(MAX_REQUEST);

  @Override
  public void encode(final ChannelHandlerContext ctx, final Request<?> request,
                     final List<Object> out)
      throws Exception {
    workingBuffer.clear();
    final ByteBuf message = request.writeRequest(ctx.alloc(), workingBuffer);
    out.add(message);
  }

}

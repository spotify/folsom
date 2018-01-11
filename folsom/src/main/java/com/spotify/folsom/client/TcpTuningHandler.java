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
/**
 * Copyright (C) 2012-2014 Spotify AB
 */

package com.spotify.folsom.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannelConfig;

/**
 * A channel handler that sets up TCP keepalive and disables Nagle's algorithm. Removes itself from
 * the pipeline after it is done.
 */
public class TcpTuningHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    final SocketChannelConfig config = (SocketChannelConfig) ctx.channel().config();

    // Disable Nagle's algorithm
    config.setTcpNoDelay(true);

    // Setup TCP keepalive
    config.setKeepAlive(true);

    super.channelActive(ctx);

    // Our work is done
    ctx.channel().pipeline().remove(this);
  }
}

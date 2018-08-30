/*
 * Copyright (c) 2018 Spotify AB
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

package com.spotify.folsom.authenticate;

import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.DefaultRawMemcacheClient;
import com.spotify.folsom.guava.HostAndPort;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.Charset;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

public class AuthenticatingClient {

  public static CompletionStage<RawMemcacheClient> connect(
      final HostAndPort address,
      final int outstandingRequestLimit,
      final boolean binary,
      final Authenticator authenticator,
      final Executor executor,
      final long timeoutMillis,
      final Charset charset,
      final Metrics metrics,
      final int maxSetLength,
      final EventLoopGroup eventLoopGroup,
      final Class<? extends Channel> channelClass) {

    if(!binary && authenticator != null && !(authenticator instanceof NoopAuthenticator)) {
      throw new IllegalArgumentException("Authentication can only be used for binary clients.");
    }

    CompletionStage<RawMemcacheClient> client = DefaultRawMemcacheClient.connect(
        address, outstandingRequestLimit,
        binary, executor, timeoutMillis, charset,
        metrics, maxSetLength, eventLoopGroup, channelClass);

    return authenticator.authenticate(client);
  }
}

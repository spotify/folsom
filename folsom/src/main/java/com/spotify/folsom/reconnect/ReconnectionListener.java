/*
 * Copyright (c) 2023 Spotify AB
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
package com.spotify.folsom.reconnect;

import com.spotify.folsom.guava.HostAndPort;
import javax.annotation.Nullable;

/**
 * A listener for each reconnection event. This will be called more often than the {@link
 * com.spotify.folsom.ConnectionChangeListener ConnectionChangeListener}s.
 *
 * <p>If you implement this interface manually, expect there to be changes in <b>minor versions</b>
 * which can cause source-incompatibility. If you want to implement this interface without this
 * hassle, extend {@link AbstractReconnectionListener} instead.
 */
public interface ReconnectionListener {

  /**
   * A connection to Memcached was attempted, but ultimately failed.
   *
   * @param cause why the connection failed, never {@code null}
   */
  void connectionFailure(final Throwable cause);

  /**
   * The reconnection was cancelled. This can be because the client was shut down between the
   * connection and clean up upon a retry, or because a connection couldn't be acquired to begin
   * with.
   */
  void reconnectionCancelled();

  /**
   * A reconnect was successful, and a new connection has been set up.
   *
   * @param address the address to which the client connected, never {@code null}
   * @param attempt which attempt it succeeded on; this is 1-indexed (i.e. 1st attempt is 1, 2nd is
   *     2)
   * @param willStayConnected whether the connection will immediately close after this, often due to
   *     race conditions
   */
  void reconnectionSuccessful(
      final HostAndPort address, final int attempt, final boolean willStayConnected);

  /**
   * A connection to Memcached was lost after having been acquired.
   *
   * <p>This will be called regardless of if it was shut down intentionally, or in error.
   *
   * @param cause the cause of losing the connection, can be {@code null}
   * @param address the address to the Memcached node we were connected, never {@code null}
   */
  void connectionLost(final @Nullable Throwable cause, final HostAndPort address);

  /**
   * A new reconnection has been queued on the executor. This is done after a failure occurred.
   *
   * <p>If the client is shut down between this call and the actual connection, you will never see a
   * follow-up to any success or failure method.
   *
   * @param cause the cause of the reconnection, i.e. "what failed to cause this?", never {@code
   *     null}
   * @param address the address to the Memcached node we will connect to, never {@code null}
   * @param backOffMillis how many milliseconds we will wait before we attempt again, never {@code
   *     null}
   * @param attempt which attempt we are on; this is 1-indexed (i.e. 1st attempt is 1, 2nd is 2)
   */
  void reconnectionQueuedFromError(
      final Throwable cause,
      final HostAndPort address,
      final long backOffMillis,
      final int attempt);
}

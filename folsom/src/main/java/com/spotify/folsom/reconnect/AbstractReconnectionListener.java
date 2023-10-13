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
 * com.spotify.folsom.ConnectionChangeListener ConnectionChangeListener}s. For more information on
 * this, see {@link ReconnectionListener}.
 *
 * <p>This class acts as a stable interface for {@link ReconnectionListener}. While {@link
 * ReconnectionListener} can have source-incompatible changes in minor versions, this cannot do
 * that.
 *
 * @see ReconnectionListener
 */
public abstract class AbstractReconnectionListener implements ReconnectionListener {
  /** {@inheritDoc} */
  @Override
  public void connectionFailure(final Throwable cause) {
    // No-op.
  }

  /** {@inheritDoc} */
  @Override
  public void connectionLost(final @Nullable Throwable cause, final HostAndPort address) {
    // No-op.
  }

  /** {@inheritDoc} */
  @Override
  public void reconnectionSuccessful(
      final HostAndPort address, final int attempt, final boolean willStayConnected) {
    // No-op.
  }

  /** {@inheritDoc} */
  @Override
  public void reconnectionCancelled() {
    // No-op.
  }

  /** {@inheritDoc} */
  @Override
  public void reconnectionQueuedFromError(
      final Throwable cause,
      final HostAndPort address,
      final long backOffMillis,
      final int attempt) {
    // No-op.
  }
}

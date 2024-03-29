/*
 * Copyright (c) 2015 Spotify AB
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

package com.spotify.folsom;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementations of this interface has a notion of connectedness to a remote and the ability to
 * notify listeners of connection state changes.
 */
public interface ObservableClient {
  /**
   * Register for connection change events. This should trigger at least once for every connection
   * change. You should immediately get an initial callback, so that if you are creating a
   * CompletionStage looking for a connection state that has already been reached it will return
   * immediately.
   *
   * @param listener the listener to notify of connection changes
   */
  void registerForConnectionChanges(ConnectionChangeListener listener);

  /**
   * Unregister the provided listener so that it no longer receives connection change callbacks.
   *
   * @param listener the listener to unregister.
   */
  void unregisterForConnectionChanges(ConnectionChangeListener listener);

  /**
   * Is the client connected to a server?
   *
   * @return true if the client is connected
   */
  boolean isConnected();

  /** @return completes when at least one underlying client is connected */
  default CompletionStage<Void> connectFuture() {
    return ConnectFuture.connectFuture(this);
  }

  /** @return completes when at least one underlying client is disconnected */
  default CompletionStage<Void> disconnectFuture() {
    return ConnectFuture.disconnectFuture(this);
  }

  /** @return completes when all underlying clients are connected */
  default CompletionStage<Void> fullyConnectedFuture() {
    return ConnectFuture.fullyConnectedFuture(this);
  }

  /** @return completes when all underlying clients are disconnected */
  default CompletionStage<Void> fullyDisconnectFuture() {
    return ConnectFuture.fullyDisconnectedFuture(this);
  }

  /** Wait for at least one underlying client to be connected */
  default void awaitConnected(final long waitTime, final TimeUnit unit)
      throws TimeoutException, InterruptedException {
    awaitFuture(connectFuture(), waitTime, unit);
  }

  /** Wait for at least one underlying client to be disconnected */
  default void awaitDisconnected(final long waitTime, final TimeUnit unit)
      throws TimeoutException, InterruptedException {
    awaitFuture(disconnectFuture(), waitTime, unit);
  }

  /** Wait for all underlying clients to be connected */
  default void awaitFullyConnected(final long waitTime, final TimeUnit unit)
      throws TimeoutException, InterruptedException {
    awaitFuture(fullyConnectedFuture(), waitTime, unit);
  }

  /** Wait for all underlying clients to be disconnected */
  default void awaitFullyDisconnected(final long waitTime, final TimeUnit unit)
      throws TimeoutException, InterruptedException {
    awaitFuture(fullyDisconnectFuture(), waitTime, unit);
  }

  default void awaitFuture(
      final CompletionStage<Void> future, final long waitTime, final TimeUnit unit)
      throws InterruptedException, TimeoutException {
    try {
      future.toCompletableFuture().get(waitTime, unit);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof MemcacheAuthenticationException) {
        throw (MemcacheAuthenticationException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the unrecoverable connection failure, if any.
   *
   * @return null, if there's no connection failure
   */
  Throwable getConnectionFailure();

  /**
   * How many actual socket connections do we have, including currently disconnected clients.
   *
   * @return the number of total connections
   */
  int numTotalConnections();

  /**
   * How many active socket connections do we have (i.e. not disconnected)
   *
   * @return the number of active connections
   */
  int numActiveConnections();

  default void notifyConnectionChange() {}
}

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

package com.spotify.folsom;

import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.client.Request;

/**
 * A raw memcache client, mostly useful internally
 */
public interface RawMemcacheClient {

  <T> ListenableFuture<T> send(Request<T> request);

  /**
   * Shut down the client. After the completion of this, the client is no longer possible to use
   * @return A future representing completion of the request
   */
  ListenableFuture<Void> shutdown();

  /**
   * Is the client connected to a server?
   * @return true if the client is connected
   */
  boolean isConnected();

  /**
   * How many actual socket connections do we have, including currently disconnected clients.
   * @return the number of total connections
   */
  int numTotalConnections();

  /**
   * How many active socket connections do we have (i.e. not disconnected)
   * @return the number of active connections
   */
  int numActiveConnections();
}

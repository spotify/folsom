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

import java.util.concurrent.CompletionStage;
import com.spotify.folsom.client.Request;

/**
 * A raw memcache client, mostly useful internally
 */
public interface RawMemcacheClient extends ObservableClient {

  <T> CompletionStage<T> send(Request<T> request);

  /**
   * Shut down the client. Use {@link #registerForConnectionChanges(ConnectionChangeListener)} to
   * to get notified when it has (possibly) finished shutting down
   */
  void shutdown();

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

/*
 * Copyright (c) 2015-2018 Spotify AB
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

import java.util.List;

public interface MemcacheClient<V> extends ObservableClient {

  /**
   * Set a key in memcache to the provided value, with the specified TTL
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> set(String key, V value, int ttl);

  /**
   * Compare and set a key in memcache to the provided value, with the specified TTL
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @param cas The CAS value, must match the value on the server for the set to go through
   * @return A future representing completion of the request.
   */
  CompletionStage<MemcacheStatus> set(String key, V value, int ttl, long cas);

  /**
   * Delete the provided key
   *
   * @param key Key, must not be null
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> delete(String key);

  /**
   * Add a key in memcache with the provided value, with the specified TTL. Key must not exist
   * in memcache
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> add(String key, V value, int ttl);

  /**
   * Replace a key in memcache with the provided value, with the specified TTL. Key must exist
   * in memcache
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> replace(String key, V value, int ttl);

  CompletionStage<MemcacheStatus> append(String key, V value);

  CompletionStage<MemcacheStatus> prepend(String key, V value);

  /**
   * Get the value for the provided key
   * @param key The key, must not be null
   * @return A future representing completion of the request, with the value, or null if the key
   *         does not exist
   */
  CompletionStage<V> get(String key);

  /**
   * Get the value for the provided key, including the CAS value
   *
   * @param key First key, must not be null
   * @return A future representing completion of the request, with the value, including the CAS
   * value, or null if the value does not exists.
   */
  CompletionStage<GetResult<V>> casGet(String key);

  /**
   * Get the value for the provided keys
   *
   * @param keys Keys, must not be null, nor must any key in the list
   * @return A future representing completion of the request, with the values. Any non existing
   * values will be null. Order will be maintained from the input keys
   */
  CompletionStage<List<V>> get(List<String> keys);

  /**
   * Get the value for the provided keys
   *
   * @param keys Keys, must not be null, nor must any key in the list
   * @return A future representing completion of the request, with the values,
   * including the CAS value. Any non existing
   * values will be null. Order will be maintained from the input keys
   */
  CompletionStage<List<GetResult<V>>> casGet(List<String> keys);

  /**
   * Sets the expiration for the provided key
   *
   * @param key First key, must not be null
   * @param ttl The TTL in seconds
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> touch(String key, int ttl);

  /**
   * Flushes all entries in the storage
   *
   * @param delay The flush delay in seconds.
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> flushAll(int delay);

  /**
   * Shut down the client.
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

  /**
   * Note: This is typically only useful for testing and debugging
   * @return the underlying raw memcache client.
   */
  RawMemcacheClient getRawMemcacheClient();

}

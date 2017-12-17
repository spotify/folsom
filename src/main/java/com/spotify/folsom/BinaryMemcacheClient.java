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

import java.util.List;

/**
 * A memcache client using the binary protocol
 *
 * @param <V> The value type for all operations
 */
public interface BinaryMemcacheClient<V> extends MemcacheClient<V> {

  /**
   * Add a key in memcache with the provided value, with the specified TTL. Key must not exist
   * in memcache
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @param cas The CAS value, must match the value on the server for the set to go through
   * @return A future representing completion of the request, containing the new CAS value
   */
  CompletionStage<MemcacheStatus> add(String key, V value, int ttl, long cas);

  /**
   * Replace a key in memcache with the provided value, with the specified TTL. Key must exist
   * in memcache
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @param cas The CAS value, must match the value on the server for the set to go through
   * @return A future representing completion of the request, containing the new CAS value
   */
  CompletionStage<MemcacheStatus> replace(String key, V value, int ttl, long cas);

  /**
   * Get the value for the provided key and sets the expiration
   * @param ttl The TTL in seconds
   * @param key The key, must not be null
   * @return A future representing completion of the request, with the value, or null if the key
   *         does not exist
   */
  CompletionStage<V> getAndTouch(String key, int ttl);

  /**
   * Get the values for the provided keys and sets the expiration
   *
   * @param keys Keys, must not be null, nor must any key in the list
   * @param ttl  The TTL in seconds
   * @return A future representing completion of the request, with the values. Any non existing
   * values will be null. Order will be maintained from the input keys
   */
  CompletionStage<List<V>> getAndTouch(List<String> keys, int ttl);

  /**
   * Get the value for the provided key, including the CAS value, and sets the expiration
   *
   * @param key First key, must not be null
   * @param ttl The TTL in seconds
   * @return A future representing completion of the request, with the value, including the CAS
   * value, or null if the value does not exists.
   */
  CompletionStage<GetResult<V>> casGetAndTouch(String key, int ttl);

  /**
   * Increment a counter for the provided key
   *
   * @param key     The key, must not be null
   * @param by      The value to increment the counter by
   * @param initial The initial value if the key does not exist
   * @param ttl     The TTL, in seconds
   * @return A future representing completion of the request, with the new value of the counter
   */
  CompletionStage<Long> incr(String key, long by, long initial, int ttl);

  /**
   * Decrement a counter for the provided key
   *
   * @param key     The key, must not be null
   * @param by      The value to decrement the counter by
   * @param initial The initial value if the key does not exist
   * @param ttl     The TTL, in seconds
   * @return A future representing completion of the request, with the new value of the counter
   */
  CompletionStage<Long> decr(String key, long by, long initial, int ttl);

  CompletionStage<MemcacheStatus> append(String key, V value, long cas);

  CompletionStage<MemcacheStatus> prepend(String key, V value, long cas);

  /**
   * Send a noop request
   *
   * @return A future representing completion of the request
   */
  CompletionStage<Void> noop();
}

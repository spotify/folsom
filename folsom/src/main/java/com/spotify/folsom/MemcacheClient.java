/*
 * Copyright (c) 2015-2019 Spotify AB
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

import com.spotify.folsom.client.Flags;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface MemcacheClient<V> extends ObservableClient {

  /**
   * Set a key in memcache to the provided value, with the specified TTL
   *
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> set(String key, V value, int ttl);

  /**
   * Set a key in memcache to the provided value, with the specified TTL
   *
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param flags Memcached flags
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> set(String key, V value, int ttl, Flags flags);

  /**
   * Compare and set a key in memcache to the provided value, with the specified TTL
   *
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @param cas The CAS value, must match the value on the server for the set to go through
   * @return A future representing completion of the request.
   */
  CompletionStage<MemcacheStatus> set(String key, V value, int ttl, long cas);

  /**
   * Compare and set a key in memcache to the provided value, with the specified TTL
   *
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @param cas The CAS value, must match the value on the server for the set to go through
   * @param flags Memcached flags
   * @return A future representing completion of the request.
   */
  CompletionStage<MemcacheStatus> set(String key, V value, int ttl, long cas, Flags flags);

  /**
   * Delete the provided key
   *
   * @param key Key, must not be null
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> delete(String key);

  /**
   * Deletes a key with CAS check.
   *
   * @param key The key, must not be null
   * @param cas The CAS value, must match the value on the server for the set to go through
   * @return A future representing completion of the request.
   */
  CompletionStage<MemcacheStatus> delete(String key, long cas);

  /**
   * Delete the provided key on all memcached instances. This is typically only useful for a
   * multi-instance setup (using Ketama).
   *
   * @param key Key, must not be null
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> deleteAll(String key);

  /**
   * Add a key in memcache with the provided value, with the specified TTL. Key must not exist in
   * memcache
   *
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> add(String key, V value, int ttl);

  /**
   * Add a key in memcache with the provided value, with the specified TTL. Key must not exist in
   * memcache
   *
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @param flags Memcached flags
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> add(String key, V value, int ttl, Flags flags);

  /**
   * Replace a key in memcache with the provided value, with the specified TTL. Key must exist in
   * memcache
   *
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> replace(String key, V value, int ttl);

  /**
   * Replace a key in memcache with the provided value, with the specified TTL. Key must exist in
   * memcache
   *
   * @param key The key, must not be null
   * @param value The value, must not be null
   * @param ttl The TTL in seconds
   * @param flags Memcached flags
   * @return A future representing completion of the request
   */
  CompletionStage<MemcacheStatus> replace(String key, V value, int ttl, Flags flags);

  CompletionStage<MemcacheStatus> append(String key, V value);

  CompletionStage<MemcacheStatus> prepend(String key, V value);

  /**
   * Get the value for the provided key
   *
   * @param key The key, must not be null
   * @return A future representing completion of the request, with the value, or null if the key
   *     does not exist
   */
  CompletionStage<V> get(String key);

  /**
   * Get the value for the provided key, including the CAS value and flags
   *
   * @param key First key, must not be null
   * @return A future representing completion of the request, with the value, including the CAS
   *     value and flags, or null if the value does not exists.
   */
  CompletionStage<GetResult<V>> casGet(String key);

  /**
   * Get the value for the provided key, including the CAS value and flags
   *
   * @param key First key, must not be null
   * @return A future representing completion of the request, with the value, including the CAS
   *     value and flags, or null if the value does not exists.
   */
  default CompletionStage<GetResult<V>> getWithFlags(String key) {
    return casGet(key);
  }

  /**
   * Get the value for the provided keys
   *
   * @param keys Keys, must not be null, nor must any key in the list
   * @return A future representing completion of the request, with the values. Any non existing
   *     values will be null. Order will be maintained from the input keys
   */
  CompletionStage<List<V>> get(List<String> keys);

  /**
   * Get the value for the provided keys
   *
   * @param keys Keys, must not be null, nor must any key in the list
   * @return A future representing completion of the request, with the values, including the CAS
   *     value and flags. Any non existing values will be null. Order will be maintained from the
   *     input keys
   */
  CompletionStage<List<GetResult<V>>> casGet(List<String> keys);

  /**
   * Get the value for the provided keys
   *
   * @param keys Keys, must not be null, nor must any key in the list
   * @return A future representing completion of the request, with the values, including the CAS
   *     value and flags. Any non existing values will be null. Order will be maintained from the
   *     input keys
   */
  default CompletionStage<List<GetResult<V>>> getWithFlags(List<String> keys) {
    return casGet(keys);
  }

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
   * Get raw statistics from the memcached instances. The return type is a map from hostname:port to
   * a statistics container.
   *
   * <p>The reason for including potentially multiple statistics containers is to support the ketama
   * use-case where there may be multiple memcached instances.
   *
   * <p>If any of the instances fail to return statistics, the exception will be propagated.
   *
   * <p>Valid keys are:
   *
   * <ul>
   *   <li>Empty string: ""
   *   <li>"slabs"
   *   <li>"sizes"
   *   <li>"items"
   *   <li>"settings"
   * </ul>
   *
   * @return a map of hostname:port to a statistics container
   * @param key - statistics key.
   */
  CompletionStage<Map<String, MemcachedStats>> getStats(String key);

  /** Shut down the client. */
  void shutdown();

  /**
   * Note: This is typically only useful for testing and debugging
   *
   * @return the underlying raw memcache client.
   */
  RawMemcacheClient getRawMemcacheClient();
}

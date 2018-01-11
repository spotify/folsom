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

/**
 * A memcache client using the ascii protocol
 *
 * @param <V> The value type for all operations
 */
public interface AsciiMemcacheClient<V> extends MemcacheClient<V> {

  /**
   * Increment a counter for the provided key
   *
   * @param key     The key, must not be null
   * @param by      The value to increment the counter by
   * @return A future representing completion of the request, with the new value of the counter
   */
  CompletionStage<Long> incr(String key, long by);

  /**
   * Decrement a counter for the provided key
   *
   * @param key     The key, must not be null
   * @param by      The value to decrement the counter by
   * @return A future representing completion of the request, with the new value of the counter
   */
  CompletionStage<Long> decr(String key, long by);
}

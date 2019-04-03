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
package com.spotify.folsom.client;

import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.MemcachedStats;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface AllRequest<T> extends Request<T> {

  default CompletionStage<T> preMerge(CompletionStage<T> stage) {
    return stage;
  }

  T merge(List<T> results);

  static MemcacheStatus mergeMemcacheStatus(List<MemcacheStatus> results) {
    return results
        .stream()
        .filter(status -> status != MemcacheStatus.OK)
        .findFirst()
        .orElse(MemcacheStatus.OK);
  }

  static Map<String, MemcachedStats> mergeStats(List<Map<String, MemcachedStats>> results) {
    return results
        .stream()
        .reduce(
            (dest, other) -> {
              dest.putAll(other);
              return dest;
            })
        .get();
  }
}

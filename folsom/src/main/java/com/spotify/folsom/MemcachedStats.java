/*
 * Copyright (c) 2019 Spotify AB
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

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Objects;

public class MemcachedStats {
  private final Map<String, String> stats;

  public MemcachedStats(final Map<String, String> stats) {
    this.stats = requireNonNull(stats);
  }

  public Map<String, String> getStats() {
    return stats;
  }

  @Override
  public String toString() {
    return "MemcachedStats{" + "stats=" + stats + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MemcachedStats that = (MemcachedStats) o;
    return stats.equals(that.stats);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stats);
  }
}

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

import static java.util.Objects.requireNonNull;

public class GetResult<V> {

  private final V value;
  private final long cas;

  private GetResult(final V value, final long cas) {
    this.value = requireNonNull(value);
    this.cas = cas;
  }

  public static <V> GetResult<V> success(final V value, final long cas) {
    requireNonNull(value, "value");
    return new GetResult<>(value, cas);
  }

  public V getValue() {
    return value;
  }

  public long getCas() {
    return cas;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GetResult<?> getResult = (GetResult<?>) o;

    if (cas != getResult.cas) return false;
    if (!value.equals(getResult.value)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = value.hashCode();
    result = 31 * result + (int) (cas ^ (cas >>> 32));
    return result;
  }
}

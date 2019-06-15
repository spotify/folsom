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

package com.spotify.folsom.opencensus;

import com.spotify.folsom.Tracer;

/** Starting point for the OpenCensus tracing implementation for Folsom */
public class OpenCensus {

  private static final boolean DEFAULT_INCLUDE_KEYS = true;
  private static final boolean DEFAULT_INCLUDE_VALUES = false;

  /**
   * Get tracer, usually to be provided to {@link
   * com.spotify.folsom.MemcacheClientBuilder#withTracer(Tracer)}.
   */
  public static Tracer tracer() {
    return new OpenCensusTracer(DEFAULT_INCLUDE_KEYS, DEFAULT_INCLUDE_VALUES);
  }

  public static class Builder {
    private boolean includeKeys = DEFAULT_INCLUDE_KEYS;
    private boolean includeValues = DEFAULT_INCLUDE_VALUES;

    /**
     * If true, the key used for an operation will be included as a span attribute. Default to true.
     */
    public Builder withIncludeKeys(final boolean includeKeys) {
      this.includeKeys = includeKeys;
      return this;
    }

    /**
     * If true, the value used for an operation will be included as a span attribute. Default to
     * false.
     *
     * <p>Note that enabling tracing with large values may add excessive load on the tracing system.
     */
    public Builder withIncludeValues(final boolean includeValues) {
      this.includeValues = includeValues;
      return this;
    }

    public Tracer build() {
      return new OpenCensusTracer(includeKeys, includeValues);
    }
  }
}

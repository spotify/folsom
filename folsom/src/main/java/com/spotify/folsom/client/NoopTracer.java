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

package com.spotify.folsom.client;

import com.spotify.folsom.Span;
import com.spotify.folsom.Tracer;
import java.util.concurrent.CompletionStage;

public class NoopTracer implements Tracer {

  public static final NoopTracer INSTANCE = new NoopTracer();
  private static final Span SPAN =
      new Span() {
        @Override
        public Span value(final byte[] value) {
          return this;
        }

        @Override
        public Span success() {
          return this;
        }

        @Override
        public Span failure() {
          return this;
        }

        @Override
        public void close() {}
      };

  private NoopTracer() {}

  /** Create scoped span. Must be closed in order to end scope. */
  public Span span(final String name, final CompletionStage<?> future, final String operation) {
    return SPAN;
  }

  @Override
  public Span span(
      final String name,
      final CompletionStage<?> future,
      final String operation,
      final String key) {
    return SPAN;
  }

  @Override
  public Span span(
      final String name,
      final CompletionStage<?> future,
      final String operation,
      final String key,
      final byte[] value) {
    return SPAN;
  }
}

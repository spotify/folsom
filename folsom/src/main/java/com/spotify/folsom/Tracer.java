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

import java.util.concurrent.CompletionStage;

public interface Tracer {

  /** Create scoped span. Span will be closed when future completes. */
  Span span(final String name, final CompletionStage<?> future, final String operation);

  /** Create scoped span. Span will be closed when future completes. */
  Span span(
      final String name, final CompletionStage<?> future, final String operation, final String key);

  /** Create scoped span. Span will be closed when future completes. */
  Span span(
      final String name,
      final CompletionStage<?> future,
      final String operation,
      final String key,
      final byte[] value);
}

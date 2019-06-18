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

import static io.opencensus.trace.AttributeValue.stringAttributeValue;
import static io.opencensus.trace.Span.Kind.CLIENT;
import static java.util.Objects.requireNonNull;

import com.spotify.folsom.GetResult;
import com.spotify.folsom.Span;
import com.spotify.folsom.Tracer;
import io.opencensus.trace.Tracing;
import java.util.concurrent.CompletionStage;

class OpenCensusTracer implements Tracer {

  private static final io.opencensus.trace.Tracer TRACER = Tracing.getTracer();

  private final boolean includeKeys;
  private final boolean includeValues;

  OpenCensusTracer(final boolean includeKeys, final boolean includeValues) {
    this.includeKeys = includeKeys;
    this.includeValues = includeValues;
  }

  @Override
  public Span span(final String name, final CompletionStage<?> future, final String operation) {
    return internalSpan(name, future, operation, null, null);
  }

  @Override
  public Span span(
      final String name,
      final CompletionStage<?> future,
      final String operation,
      final String key) {
    requireNonNull(key);
    return internalSpan(name, future, operation, key, null);
  }

  @Override
  public Span span(
      final String name,
      final CompletionStage<?> future,
      final String operation,
      final String key,
      final byte[] value) {
    requireNonNull(key);
    requireNonNull(value);
    return internalSpan(name, future, operation, key, value);
  }

  @SuppressWarnings("MustBeClosedChecker")
  private Span internalSpan(
      final String name,
      final CompletionStage<?> future,
      final String operation,
      final String key,
      final byte[] value) {
    requireNonNull(name);
    requireNonNull(future);
    requireNonNull(operation);

    final io.opencensus.trace.Span ocSpan =
        TRACER.spanBuilder(name).setSpanKind(CLIENT).startSpan();

    ocSpan.putAttribute("component", stringAttributeValue("folsom"));
    ocSpan.putAttribute("peer.service", stringAttributeValue("memcache"));
    ocSpan.putAttribute("operation", stringAttributeValue(operation));
    if (key != null && includeKeys) {
      ocSpan.putAttribute("key", stringAttributeValue(key));
    }

    final Span span = new OpenCensusSpan(ocSpan, includeValues);

    if (value != null) {
      span.value(value);
    }

    future.whenComplete(
        (result, t) -> {
          if (result instanceof GetResult) {
            final GetResult<byte[]> getResult = (GetResult) result;
            span.value(getResult.getValue());
          }

          if (t == null) {
            span.success();
          } else {
            span.failure();
          }
          span.close();
        });

    return span;
  }
}

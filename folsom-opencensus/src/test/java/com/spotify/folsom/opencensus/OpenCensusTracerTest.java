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

import static io.opencensus.trace.AttributeValue.longAttributeValue;
import static io.opencensus.trace.AttributeValue.stringAttributeValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import com.google.common.io.BaseEncoding;
import com.spotify.folsom.Span;
import io.opencensus.common.Scope;
import io.opencensus.testing.export.TestHandler;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.config.TraceParams;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.samplers.Samplers;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;

public class OpenCensusTracerTest {

  private static final String OPERATION = "op";
  private static final String KEY = "k";
  private static final byte[] VALUE = "v".getBytes(StandardCharsets.US_ASCII);
  private static final BaseEncoding HEX = BaseEncoding.base16().lowerCase();

  private final TestHandler handler = new TestHandler();

  @Before
  public void setUp() {
    // sample all
    final TraceConfig traceConfig = Tracing.getTraceConfig();
    final TraceParams activeTraceParams = traceConfig.getActiveTraceParams();
    traceConfig.updateActiveTraceParams(
        activeTraceParams.toBuilder().setSampler(Samplers.alwaysSample()).build());

    Tracing.getExportComponent().getSpanExporter().registerHandler("test", handler);
  }

  @Test
  public void traceWithCompleteOnSameThread() {
    trace(future -> future.complete(null), Status.OK);
  }

  @Test
  public void traceWithCompleteOnNewThread() {
    trace(
        future -> {
          new Thread(() -> future.complete(null)).start();
        },
        Status.OK);
  }

  @Test
  public void traceWithFail() {
    trace(future -> future.completeExceptionally(new RuntimeException()), Status.UNKNOWN);
  }

  private void trace(
      final Consumer<CompletableFuture<Void>> completionist, final Status expectedStatus) {
    final Tracer tracer = Tracing.getTracer();

    try (final Scope scope = tracer.spanBuilder("outer").startScopedSpan()) {
      final SpanContext outerCtx = tracer.getCurrentSpan().getContext();

      final CompletableFuture<Void> future = new CompletableFuture<>();

      final com.spotify.folsom.Tracer tt =
          new OpenCensus.Builder().withIncludeKeys(true).withIncludeValues(true).build();

      final Span span = tt.span("span", future, OPERATION, KEY, VALUE);
      io.opencensus.trace.Span inner = tracer.getCurrentSpan();

      // as the span operation is async, the span must not set itself as part of the thread context
      assertSame(outerCtx, inner.getContext());

      // complete the async operation
      completionist.accept(future);

      final SpanContext afterCtx = tracer.getCurrentSpan().getContext();

      assertSame(outerCtx, afterCtx);
    }

    final List<SpanData> spans = handler.waitForExport(2);

    final SpanData outer = findSpan(spans, "outer");
    final SpanData inner = findSpan(spans, "span");

    assertEquals(outer.getContext().getTraceId(), inner.getContext().getTraceId());
    assertEquals(outer.getContext().getSpanId(), inner.getParentSpanId());
    final Map<String, AttributeValue> attributes = inner.getAttributes().getAttributeMap();
    assertEquals(stringAttributeValue("folsom"), attributes.get("component"));
    assertEquals(stringAttributeValue("memcache"), attributes.get("peer.service"));
    assertEquals(stringAttributeValue("op"), attributes.get("operation"));
    assertEquals(stringAttributeValue(KEY), attributes.get("key"));
    assertEquals(longAttributeValue(VALUE.length), attributes.get("value_size_bytes"));
    assertEquals(stringAttributeValue(HEX.encode(VALUE)), attributes.get("value_hex"));
    assertEquals(expectedStatus, inner.getStatus());
  }

  @Test
  public void spanCreationFailureIsNotFatal() {
    final IllegalArgumentException failure =
        new IllegalArgumentException("Invalid tracestate key: 2487701@nr");
    final FailingTracer failingTracer = new FailingTracer(failure);

    try {
      failingTracer.spanBuilder("span");
      fail("FailingTracer should throw");
    } catch (final IllegalArgumentException e) {
      assertSame(failure, e);
    }

    final com.spotify.folsom.Tracer tracer = new OpenCensusTracer(failingTracer, true, true);

    final CompletableFuture<String> future = new CompletableFuture<>();

    final Span span = tracer.span("span", future, OPERATION);
    tracer.span("span", future, OPERATION, KEY);
    tracer.span("span", future, OPERATION, KEY, VALUE);

    future.complete("value");

    assertEquals("value", future.join());

    // the returned span is still usable
    assertSame(span, span.value(VALUE).success());
    span.close();
  }

  private static final class FailingTracer extends Tracer {

    private final RuntimeException failure;

    FailingTracer(final RuntimeException failure) {
      this.failure = failure;
    }

    @Override
    public SpanBuilder spanBuilderWithExplicitParent(
        final String spanName, final io.opencensus.trace.Span parent) {
      throw failure;
    }

    @Override
    public SpanBuilder spanBuilderWithRemoteParent(
        final String spanName, final SpanContext parentContext) {
      throw failure;
    }
  }

  private SpanData findSpan(final List<SpanData> spans, final String name) {
    return spans.stream().filter(s -> s.getName().equals(name)).findFirst().get();
  }
}

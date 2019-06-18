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

import com.google.common.io.BaseEncoding;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import io.opencensus.common.Scope;
import io.opencensus.testing.export.TestHandler;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.config.TraceParams;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.samplers.Samplers;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

public class OpenCensusTest {

  private static final BaseEncoding HEX = BaseEncoding.base16().lowerCase();

  private static final String ROOT_SPAN_NAME = "trace-test";
  public static final String KEY = "hello";
  public static final String VALUE = "world";

  private final TestHandler handler = new TestHandler();

  private GenericContainer container;
  private MemcacheClientBuilder<String> builder;

  @Before
  public void setUp() {
    final TraceConfig traceConfig = Tracing.getTraceConfig();
    final TraceParams activeTraceParams = traceConfig.getActiveTraceParams();
    traceConfig.updateActiveTraceParams(
        activeTraceParams.toBuilder().setSampler(Samplers.alwaysSample()).build());

    Tracing.getExportComponent().getSpanExporter().registerHandler("test", handler);

    container = new GenericContainer("bitnami/memcached:1.5.12").withExposedPorts(11211);
    container.start();

    builder =
        MemcacheClientBuilder.newStringClient()
            .withAddress(container.getContainerIpAddress(), container.getFirstMappedPort())
            .withTracer(OpenCensus.tracer());
  }

  @Test
  public void traceAscii() throws TimeoutException, InterruptedException {
    trace(builder.connectAscii());
  }

  @Test
  public void traceBinary() throws TimeoutException, InterruptedException {
    trace(builder.connectBinary());
  }

  private void trace(final MemcacheClient<String> client)
      throws TimeoutException, InterruptedException {
    client.awaitConnected(10, TimeUnit.SECONDS);

    try (final Scope scope = Tracing.getTracer().spanBuilder(ROOT_SPAN_NAME).startScopedSpan()) {
      client.set(KEY, VALUE, 3600);
      client.get(KEY);
    }

    // wait for spans to be exported
    final List<SpanData> spans = handler.waitForExport(3);

    // find root span
    final SpanData root =
        spans.stream().filter(d -> d.getName().equals(ROOT_SPAN_NAME)).findFirst().get();

    // find first level spans
    final List<SpanData> firstLevel = getByParent(spans, root);
    assertEquals(2, firstLevel.size());

    assertSpan("folsom.set", "set", KEY, firstLevel.get(0));
    assertSpan("folsom.get", "get", KEY, firstLevel.get(1));
  }

  private void assertSpan(
      final String expectedName,
      final String expectedOperation,
      final String expectedKey,
      final SpanData actual) {
    assertEquals(expectedName, actual.getName());
    assertEquals(Status.OK, actual.getStatus());

    final Map<String, AttributeValue> attributes = actual.getAttributes().getAttributeMap();
    assertEquals(stringAttributeValue(expectedOperation), attributes.get("operation"));
    assertEquals(stringAttributeValue(expectedKey), attributes.get("key"));
    assertEquals(longAttributeValue(VALUE.length()), attributes.get("value_size_bytes"));
  }

  private List<SpanData> getByParent(final List<SpanData> spans, final SpanData parent) {
    return spans
        .stream()
        .filter(data -> parent.getContext().getSpanId().equals(data.getParentSpanId()))
        .collect(Collectors.toList());
  }
}

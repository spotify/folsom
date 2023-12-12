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

package com.spotify.folsom.client;

import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArraySet;

public class OpenTelemetryMetrics implements Metrics {

  private static final long ONE = 1L;

  private static final Attributes HIT_TAGS = tag("result", "hit");
  private static final Attributes MISS_TAGS = tag("result", "miss");
  private static final Attributes FAILURE_TAGS = tag("result", "failure");
  private static final Attributes SUCCESS_TAGS = tag("result", "success");

  private static final Attributes OPERATION_GET_TAG = tag("operation", "get");
  private static final Attributes OPERATION_MULTIGET_TAG = tag("operation", "multiget");
  private static final Attributes OPERATION_DELETE_TAG = tag("operation", "delete");
  private static final Attributes OPERATION_SET_TAG = tag("operation", "set");
  private static final Attributes OPERATION_INCRDECR_TAG = tag("operation", "increment_decrement");
  private static final Attributes OPERATION_TOUCH_TAG = tag("operation", "touch");

  private final DoubleHistogram durations;
  private final LongCounter gets;
  private final LongCounter multigets;
  private final LongCounter deletes;
  private final LongCounter sets;
  private final LongCounter incrDecrs;
  private final LongCounter touches;

  private final Set<OutstandingRequestsGauge> outstandingRequests = new CopyOnWriteArraySet<>();

  /**
   * Constructs an instance of {@link com.spotify.folsom.client.OpenTelemetryMetrics}, initializing
   * various metrics and gauges for monitoring Memcache-related activities using OpenTelemetry.
   *
   * @param openTelemetry An instance of {@link io.opentelemetry.api.OpenTelemetry} for accessing
   *     the Telemetry API.
   */
  public OpenTelemetryMetrics(final OpenTelemetry openTelemetry) {
    final Meter meter = openTelemetry.getMeter("com.spotify.folsom");
    this.durations =
        meter.histogramBuilder("memcache.requests.nanoseconds").setUnit("nanoseconds").build();
    this.gets = meter.counterBuilder("memcache.requests.get").build();
    this.multigets = meter.counterBuilder("memcache.requests.multiget").build();
    this.deletes = meter.counterBuilder("memcache.requests.delete").build();
    this.sets = meter.counterBuilder("memcache.requests.set").build();
    this.incrDecrs = meter.counterBuilder("memcache.requests.increment_decrement").build();
    this.touches = meter.counterBuilder("memcache.requests.touch").build();

    meter
        .gaugeBuilder("memcache.outstanding.requests")
        .buildWithCallback(this::outstandingRequestsObservable);
    meter
        .gaugeBuilder("memcache.global.connections")
        .buildWithCallback(this::globalConnectionsObservable);
  }

  private void outstandingRequestsObservable(final ObservableDoubleMeasurement measurement) {
    measurement.record(
        outstandingRequests.stream()
            .mapToLong(value -> (long) value.getOutstandingRequests())
            .sum());
  }

  private void globalConnectionsObservable(final ObservableDoubleMeasurement measurement) {
    measurement.record(Utils.getGlobalConnectionCount());
  }

  @Override
  public void measureGetFuture(final CompletionStage<GetResult<byte[]>> future) {
    final long start = System.nanoTime();
    future.whenComplete(
        (result, throwable) -> {
          final long duration = System.nanoTime() - start;
          durations.record(duration, OPERATION_GET_TAG);
          if (throwable == null) {
            if (result != null) {
              gets.add(ONE, HIT_TAGS);
            } else {
              gets.add(ONE, MISS_TAGS);
            }
          } else {
            gets.add(ONE, FAILURE_TAGS);
          }
        });
  }

  @Override
  public void measureMultigetFuture(final CompletionStage<List<GetResult<byte[]>>> future) {
    final long start = System.nanoTime();
    future.whenComplete(
        (results, throwable) -> {
          final long duration = System.nanoTime() - start;
          durations.record(duration, OPERATION_MULTIGET_TAG);
          if (throwable == null) {
            for (final GetResult<byte[]> result : results) {
              if (result != null) {
                multigets.add(ONE, HIT_TAGS);
              } else {
                multigets.add(ONE, MISS_TAGS);
              }
            }
          } else {
            multigets.add(ONE, FAILURE_TAGS);
          }
        });
  }

  @Override
  public void measureDeleteFuture(final CompletionStage<MemcacheStatus> future) {
    final long start = System.nanoTime();
    future.whenComplete(
        (memcacheStatus, throwable) -> {
          final long duration = System.nanoTime() - start;
          durations.record(duration, OPERATION_DELETE_TAG);
          if (throwable == null) {
            deletes.add(ONE, SUCCESS_TAGS);
          } else {
            deletes.add(ONE, FAILURE_TAGS);
          }
        });
  }

  @Override
  public void measureSetFuture(final CompletionStage<MemcacheStatus> future) {
    final long start = System.nanoTime();
    future.whenComplete(
        (memcacheStatus, throwable) -> {
          final long duration = System.nanoTime() - start;
          durations.record(duration, OPERATION_SET_TAG);
          if (throwable == null) {
            sets.add(ONE, SUCCESS_TAGS);
          } else {
            sets.add(ONE, FAILURE_TAGS);
          }
        });
  }

  @Override
  public void measureIncrDecrFuture(final CompletionStage<Long> future) {
    final long start = System.nanoTime();
    future.whenComplete(
        (memcacheStatus, throwable) -> {
          final long duration = System.nanoTime() - start;
          durations.record(duration, OPERATION_INCRDECR_TAG);
          if (throwable == null) {
            incrDecrs.add(ONE, SUCCESS_TAGS);
          } else {
            incrDecrs.add(ONE, FAILURE_TAGS);
          }
        });
  }

  @Override
  public void measureTouchFuture(final CompletionStage<MemcacheStatus> future) {
    final long start = System.nanoTime();
    future.whenComplete(
        (memcacheStatus, throwable) -> {
          final long duration = System.nanoTime() - start;
          durations.record(duration, OPERATION_TOUCH_TAG);
          if (throwable == null) {
            touches.add(ONE, SUCCESS_TAGS);
          } else {
            touches.add(ONE, FAILURE_TAGS);
          }
        });
  }

  @Override
  public void registerOutstandingRequestsGauge(final OutstandingRequestsGauge gauge) {
    outstandingRequests.add(gauge);
  }

  @Override
  public void unregisterOutstandingRequestsGauge(final OutstandingRequestsGauge gauge) {
    outstandingRequests.remove(gauge);
  }

  private static Attributes tag(final String key, final String value) {
    return Attributes.of(AttributeKey.stringKey(key), value);
  }
}

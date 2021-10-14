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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArraySet;

public class MicrometerMetrics implements Metrics {

  private final Timer getHits;
  private final Timer getMisses;
  private final Timer getFailures;

  private final Timer multigetCalls;
  private final Counter multigetHits;
  private final Counter multigetMisses;
  private final Timer multigetFailures;

  private final Timer setSuccesses;
  private final Timer setFailures;

  private final Timer deleteSuccesses;
  private final Timer deleteFailures;

  private final Timer incrDecrSuccesses;
  private final Timer incrDecrFailures;

  private final Timer touchSuccesses;
  private final Timer touchFailures;

  private final Set<OutstandingRequestsGauge> gauges = new CopyOnWriteArraySet<>();

  /** @param registry MeterRegistry */
  public MicrometerMetrics(final MeterRegistry registry) {
    this(registry, Tags.empty());
  }

  /**
   * @param registry MeterRegistry
   * @param tags Additional tags that will be added to each meter
   */
  public MicrometerMetrics(final MeterRegistry registry, final String... tags) {
    this(registry, Tags.of(tags));
  }

  /**
   * @param registry MeterRegistry
   * @param tags Additional tags that will be added to each meter
   */
  public MicrometerMetrics(final MeterRegistry registry, final Tags tags) {
    String meterName = "memcache.requests";
    this.getHits = registry.timer(meterName, tags.and("operation", "get", "result", "hits"));
    this.getMisses = registry.timer(meterName, tags.and("operation", "get", "result", "misses"));
    this.getFailures =
        registry.timer(meterName, tags.and("operation", "get", "result", "failures"));

    this.multigetCalls =
        registry.timer(meterName, tags.and("operation", "multiget", "result", "hitsOrMisses"));
    this.multigetHits =
        registry.counter(meterName, tags.and("operation", "multiget", "result", "hits"));
    this.multigetMisses =
        registry.counter(meterName, tags.and("operation", "multiget", "result", "misses"));
    this.multigetFailures =
        registry.timer(meterName, tags.and("operation", "multiget", "result", "failures"));

    this.setSuccesses =
        registry.timer(meterName, tags.and("operation", "set", "result", "successes"));
    this.setFailures =
        registry.timer(meterName, tags.and("operation", "set", "result", "failures"));

    this.deleteSuccesses =
        registry.timer(meterName, tags.and("operation", "delete", "result", "successes"));
    this.deleteFailures =
        registry.timer(meterName, tags.and("operation", "delete", "result", "failures"));

    this.incrDecrSuccesses =
        registry.timer(meterName, tags.and("operation", "incrdecr", "result", "successes"));
    this.incrDecrFailures =
        registry.timer(meterName, tags.and("operation", "incrdecr", "result", "failures"));

    this.touchSuccesses =
        registry.timer(meterName, tags.and("operation", "touch", "result", "successes"));
    this.touchFailures =
        registry.timer(meterName, tags.and("operation", "touch", "result", "failures"));

    registry.gauge(
        "memcache.outstandingRequests", tags, this, MicrometerMetrics::getOutstandingRequests);
    registry.gauge(
        "memcache.global.connections", tags, this, o -> Utils.getGlobalConnectionCount());
  }

  @VisibleForTesting
  long getOutstandingRequests() {
    return gauges.stream().mapToLong(OutstandingRequestsGauge::getOutstandingRequests).sum();
  }

  @Override
  public void measureGetFuture(CompletionStage<GetResult<byte[]>> future) {
    final long startNs = System.nanoTime();

    future.whenComplete(
        (result, t) -> {
          long duration = System.nanoTime() - startNs;
          if (t == null) {
            if (result != null) {
              getHits.record(duration, NANOSECONDS);
            } else {
              getMisses.record(duration, NANOSECONDS);
            }
          } else {
            getFailures.record(duration, NANOSECONDS);
          }
        });
  }

  @Override
  public void measureMultigetFuture(CompletionStage<List<GetResult<byte[]>>> future) {
    final long startNs = System.nanoTime();

    future.whenComplete(
        (result, t) -> {
          long duration = System.nanoTime() - startNs;
          if (t == null) {
            multigetCalls.record(duration, NANOSECONDS);

            int hits = 0;
            int total = result.size();
            for (GetResult<byte[]> getResult : result) {
              if (getResult != null) {
                hits++;
              }
            }

            multigetHits.increment(hits);
            multigetMisses.increment(total - hits);
          } else {
            multigetFailures.record(duration, NANOSECONDS);
            ;
          }
        });
  }

  @Override
  public void measureDeleteFuture(CompletionStage<MemcacheStatus> future) {
    final long startNs = System.nanoTime();

    future.whenComplete(
        (result, t) -> {
          long duration = System.nanoTime() - startNs;
          if (t == null) {
            deleteSuccesses.record(duration, NANOSECONDS);
          } else {
            deleteFailures.record(duration, NANOSECONDS);
          }
        });
  }

  @Override
  public void measureSetFuture(CompletionStage<MemcacheStatus> future) {
    final long startNs = System.nanoTime();

    future.whenComplete(
        (result, t) -> {
          long duration = System.nanoTime() - startNs;
          if (t == null) {
            setSuccesses.record(duration, NANOSECONDS);
          } else {
            setFailures.record(duration, NANOSECONDS);
          }
        });
  }

  @Override
  public void measureIncrDecrFuture(CompletionStage<Long> future) {
    final long startNs = System.nanoTime();

    future.whenComplete(
        (result, t) -> {
          long duration = System.nanoTime() - startNs;
          if (t == null) {
            incrDecrSuccesses.record(duration, NANOSECONDS);
          } else {
            incrDecrFailures.record(duration, NANOSECONDS);
          }
        });
  }

  @Override
  public void measureTouchFuture(CompletionStage<MemcacheStatus> future) {
    final long startNs = System.nanoTime();

    future.whenComplete(
        (result, t) -> {
          long duration = System.nanoTime() - startNs;
          if (t == null) {
            touchSuccesses.record(duration, NANOSECONDS);
          } else {
            touchFailures.record(duration, NANOSECONDS);
          }
        });
  }

  @Override
  public void registerOutstandingRequestsGauge(OutstandingRequestsGauge gauge) {
    gauges.add(gauge);
  }

  @Override
  public void unregisterOutstandingRequestsGauge(OutstandingRequestsGauge gauge) {
    gauges.remove(gauge);
  }
}

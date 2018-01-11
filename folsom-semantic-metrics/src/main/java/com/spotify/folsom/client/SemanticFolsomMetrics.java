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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * {@link com.spotify.folsom.Metrics} implementation using semantic-metrics.
 * Mostly a port of the YammerMetrics class.
 */
public class SemanticFolsomMetrics implements Metrics {

  private final Timer gets;
  private final Meter getHits;
  private final Meter getMisses;
  private final Meter getFailures;

  /**
   * reports the ratio of hits for (gets + multigets) to total (gets + multigets)
   */
  private final RatioGauge hitRatio;

  private final Timer sets;
  private final Meter setSuccesses;
  private final Meter setFailures;

  private final Timer multigets;
  private final Meter multigetSuccesses;
  private final Meter multigetFailures;

  /**
   * {@link #multigets} keeps track of the rate of multiget requests, this tracks the rate of total
   * elements inside the multiget request. Used in {@link #hitRatio}
   */
  private final Meter multigetItems;

  private final Timer deletes;
  private final Meter deleteSuccesses;
  private final Meter deleteFailures;

  private final Timer incrDecrs;
  private final Meter incrDecrSuccesses;
  private final Meter incrDecrFailures;

  private final Timer touches;
  private final Meter touchSuccesses;
  private final Meter touchFailures;

  private final SemanticMetricRegistry registry;
  private final MetricId id;


  public SemanticFolsomMetrics(final SemanticMetricRegistry registry, final MetricId baseMetricId) {

    this.registry = registry;

    this.id = baseMetricId.tagged("what", "memcache-results",
                                  "component", "memcache-client");

    final MetricId meterId = id.tagged("unit", "operations");

    MetricId getId = id.tagged("operation", "get");
    this.gets = registry.timer(getId);
    // successful gets are broken down by whether a result was found in the cache or not.
    // the two meters can be summed to count total number of successes.
    MetricId getMetersId = MetricId.join(getId, meterId);
    this.getHits = registry.meter(getMetersId.tagged("result", "success", "cache-result", "hit"));
    this.getMisses = registry.meter(
            getMetersId.tagged("result", "success", "cache-result", "miss"));
    this.getFailures = registry.meter(getMetersId.tagged("result", "failure"));

    // ratio of cache hits to total attempts
    hitRatio = new RatioGauge() {
      @Override
      protected Ratio getRatio() {
        return Ratio.of(getHits.getFiveMinuteRate(),
            gets.getFiveMinuteRate() + multigetItems.getFiveMinuteRate());
      }
    };
    // overwrite the 'what' as this metric doesn't make sense to be aggregated against any of the
    // other metrics
    registry.register(getId.tagged("what", "memcache-hit-ratio", "unit", "%"), hitRatio);

    MetricId setId = id.tagged("operation", "set");
    this.sets = registry.timer(setId);
    MetricId setMetersId = MetricId.join(setId, meterId);
    this.setSuccesses = registry.meter(setMetersId.tagged("result", "success"));
    this.setFailures = registry.meter(setMetersId.tagged("result", "failure"));

    MetricId multigetId = id.tagged("operation", "multiget");
    this.multigets = registry.timer(multigetId);
    MetricId multigetMetersId = MetricId.join(multigetId, meterId);
    this.multigetSuccesses = registry.meter(multigetMetersId.tagged("result", "success"));
    this.multigetFailures = registry.meter(multigetMetersId.tagged("result", "failure"));

    // doesn't seem useful to export
    this.multigetItems = new Meter();

    MetricId deleteId = id.tagged("operation", "delete");
    this.deletes = registry.timer(deleteId);
    MetricId deleteMetersId = MetricId.join(deleteId, meterId);
    this.deleteSuccesses = registry.meter(deleteMetersId.tagged("result", "success"));
    this.deleteFailures = registry.meter(deleteMetersId.tagged("result", "failure"));

    MetricId incrDecrId = id.tagged("operation", "incr-decr");
    this.incrDecrs = registry.timer(incrDecrId);
    MetricId incrDecrMetersId = MetricId.join(incrDecrId, meterId);
    this.incrDecrSuccesses = registry.meter(incrDecrMetersId.tagged("result", "success"));
    this.incrDecrFailures = registry.meter(incrDecrMetersId.tagged("result", "failure"));

    MetricId touchId = id.tagged("operation", "touch");
    this.touches = registry.timer(touchId);
    MetricId touchMetersId = MetricId.join(touchId, meterId);
    this.touchSuccesses = registry.meter(touchMetersId.tagged("result", "success"));
    this.touchFailures = registry.meter(touchMetersId.tagged("result", "failure"));

    createConnectionCounterGauge();
  }

  @Override
  public void measureGetFuture(CompletionStage<GetResult<byte[]>> future) {
    final Timer.Context context = gets.time();

    future.whenComplete((result, t) -> {
      context.stop();
      if (t == null) {
        if (result != null) {
          getHits.mark();
        } else {
          getMisses.mark();
        }
      } else {
        getFailures.mark();
      }
    });
  }

  @Override
  public void measureSetFuture(CompletionStage<MemcacheStatus> future) {
    final Timer.Context context = sets.time();

    future.whenComplete((result, t) -> {
      context.stop();
      if (t == null) {
        setSuccesses.mark();
      } else {
        setFailures.mark();
      }
    });
  }

  @Override
  public void measureMultigetFuture(CompletionStage<List<GetResult<byte[]>>> future) {
    final Timer.Context ctx = multigets.time();

    future.whenComplete((result, t) -> {
      ctx.stop();
      if (t == null) {
        multigetSuccesses.mark();
        int hits = 0;
        int total = result.size();
        for (GetResult<byte[]> aResult : result) {
          if (aResult != null) {
            hits++;
          }
        }
        getHits.mark(hits);
        getMisses.mark(total - hits);
        multigetItems.mark(total);
      } else {
        multigetFailures.mark();
      }
    });
  }

  @Override
  public void measureDeleteFuture(CompletionStage<MemcacheStatus> future) {
    final Timer.Context ctx = deletes.time();

    future.whenComplete((result, t) -> {
      ctx.stop();
      if (t == null) {
        deleteSuccesses.mark();
      } else {
        deleteFailures.mark();
      }
    });
  }

  @Override
  public void measureIncrDecrFuture(CompletionStage<Long> future) {
    final Timer.Context ctx = incrDecrs.time();

    future.whenComplete((result, t) -> {
      ctx.stop();
      if (t == null) {
        incrDecrSuccesses.mark();
      } else {
        incrDecrFailures.mark();
      }
    });
  }

  @Override
  public void measureTouchFuture(CompletionStage<MemcacheStatus> future) {
    final Timer.Context ctx = touches.time();

    future.whenComplete((result, t) -> {
      ctx.stop();
      if (t == null) {
        touchSuccesses.mark();
      } else {
        touchFailures.mark();
      }
    });
  }

  public Timer getGets() {
    return gets;
  }

  public Meter getGetHits() {
    return getHits;
  }

  public Meter getGetMisses() {
    return getMisses;
  }

  public Meter getGetFailures() {
    return getFailures;
  }

  public Timer getSets() {
    return sets;
  }

  public Meter getSetSuccesses() {
    return setSuccesses;
  }

  public Meter getSetFailures() {
    return setFailures;
  }

  public Timer getMultigets() {
    return multigets;
  }

  public Meter getMultigetSuccesses() {
    return multigetSuccesses;
  }

  public Meter getMultigetFailures() {
    return multigetFailures;
  }

  public Timer getDeletes() {
    return deletes;
  }

  public Meter getDeleteSuccesses() {
    return deleteSuccesses;
  }

  public Meter getDeleteFailures() {
    return deleteFailures;
  }

  public Timer getIncrDecrs() {
    return incrDecrs;
  }

  public Meter getIncrDecrSuccesses() {
    return incrDecrSuccesses;
  }

  public Meter getIncrDecrFailures() {
    return incrDecrFailures;
  }

  public Timer getTouches() {
    return touches;
  }

  public Meter getTouchSuccesses() {
    return touchSuccesses;
  }

  public Meter getTouchFailures() {
    return touchFailures;
  }

  public RatioGauge getHitRatio() {
    return hitRatio;
  }

  @Override
  public void registerOutstandingRequestsGauge(final OutstandingRequestsGauge gauge) {
    final MetricId gaugeId = id.tagged("what", "outstanding-requests",
                                       "unit", "requests");
    // registry doesn't allow duplicate registrations, so remove the metric if already existing
    synchronized (registry) {
      registry.remove(gaugeId);
      registry.register(gaugeId, (Gauge<Integer>) () -> gauge.getOutstandingRequests());
    }
  }

  private void createConnectionCounterGauge() {
    final MetricId gaugeId = id.tagged("what", "global-connections", "unit", "connections");

    // registry doesn't allow duplicate registrations, so remove the metric if already existing
    synchronized (registry) {
      registry.remove(gaugeId);
      registry.register(gaugeId, (Gauge<Integer>) Utils::getGlobalConnectionCount);
    }

  }
}

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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;



public class YammerMetrics implements Metrics {

  public static final String GROUP = "com.spotify.folsom";

  private final Timer gets;
  private final Meter getHits;
  private final Meter getMisses;

  private final Meter getSuccesses;
  private final Meter getFailures;

  private final Timer multigets;
  private final Meter multigetSuccesses;
  private final Meter multigetFailures;

  private final Timer sets;
  private final Meter setSuccesses;
  private final Meter setFailures;

  private final Timer deletes;
  private final Meter deleteSuccesses;
  private final Meter deleteFailures;

  private final Timer incrDecrs;
  private final Meter incrDecrSuccesses;
  private final Meter incrDecrFailures;

  private final Timer touches;
  private final Meter touchSuccesses;
  private final Meter touchFailures;

  private volatile OutstandingRequestsGauge internalOutstandingReqGauge;
  private final Gauge<Integer> outstandingRequestsGauge;

  public YammerMetrics(final MetricsRegistry registry) {
    this.gets = registry.newTimer(name("get", "requests"), SECONDS, SECONDS);
    this.getSuccesses = registry.newMeter(name("get", "successes"), "Successes", SECONDS);
    this.getHits = registry.newMeter(name("get", "hits"), "Hits", SECONDS);
    this.getMisses = registry.newMeter(name("get", "misses"), "Misses", SECONDS);
    this.getFailures = registry.newMeter(name("get", "failures"), "Failures", SECONDS);

    this.multigets = registry.newTimer(name("multiget", "requests"), SECONDS, SECONDS);
    this.multigetSuccesses = registry.newMeter(name("multiget", "successes"), "Successes", SECONDS);
    this.multigetFailures = registry.newMeter(name("multiget", "failures"), "Failures", SECONDS);

    this.sets = registry.newTimer(name("set", "requests"), SECONDS, SECONDS);
    this.setSuccesses = registry.newMeter(name("set", "successes"), "Successes", SECONDS);
    this.setFailures = registry.newMeter(name("set", "failures"), "Failures", SECONDS);

    this.deletes = registry.newTimer(name("delete", "requests"), SECONDS, SECONDS);
    this.deleteSuccesses = registry.newMeter(name("delete", "successes"), "Successes", SECONDS);
    this.deleteFailures = registry.newMeter(name("delete", "failures"), "Failures", SECONDS);

    this.incrDecrs = registry.newTimer(name("incrdecr", "requests"), SECONDS, SECONDS);
    this.incrDecrSuccesses = registry.newMeter(name("incrdecr", "successes"), "Successes", SECONDS);
    this.incrDecrFailures = registry.newMeter(name("incrdecr", "failures"), "Failures", SECONDS);

    this.touches = registry.newTimer(name("touch", "requests"), SECONDS, SECONDS);
    this.touchSuccesses = registry.newMeter(name("touch", "successes"), "Successes", SECONDS);
    this.touchFailures = registry.newMeter(name("touch", "failures"), "Failures", SECONDS);

    final MetricName gaugeName = name("outstandingRequests", "count");
    this.outstandingRequestsGauge = registry.newGauge(gaugeName, new Gauge<Integer>() {
      @Override
      public Integer value() {
        return internalOutstandingReqGauge != null ?
               internalOutstandingReqGauge.getOutstandingRequests() : 0;
      }
    });
  }

  private MetricName name(final String type, final String name) {
    return new MetricName(GROUP, type, name);
  }

  @Override
  public void measureGetFuture(ListenableFuture<GetResult<byte[]>> future) {
    final TimerContext ctx = gets.time();

    final FutureCallback<GetResult<byte[]>> metricsCallback =
            new FutureCallback<GetResult<byte[]>>() {
      @Override
      public void onSuccess(final GetResult<byte[]> result) {
        getSuccesses.mark();
        if (result != null) {
          getHits.mark();
        } else {
          getMisses.mark();
        }
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        getFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureMultigetFuture(ListenableFuture<List<GetResult<byte[]>>> future) {
    final TimerContext ctx = multigets.time();

    final FutureCallback<List<GetResult<byte[]>>> metricsCallback =
            new FutureCallback<List<GetResult<byte[]>>>() {
      @Override
      public void onSuccess(final List<GetResult<byte[]>> result) {
        multigetSuccesses.mark();
        int hits = 0;
        int total = result.size();
        for (int i = 0; i < total; i++) {
          if (result.get(i) != null) {
            hits++;
          }
        }
        getHits.mark(hits);
        getMisses.mark(total - hits);
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        multigetFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureDeleteFuture(ListenableFuture<MemcacheStatus> future) {
    final TimerContext ctx = deletes.time();

    final FutureCallback<MemcacheStatus> metricsCallback = new FutureCallback<MemcacheStatus>() {
      @Override
      public void onSuccess(final MemcacheStatus result) {
        deleteSuccesses.mark();
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        deleteFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureSetFuture(ListenableFuture<MemcacheStatus> future) {
    final TimerContext ctx = sets.time();

    final FutureCallback<MemcacheStatus> metricsCallback = new FutureCallback<MemcacheStatus>() {
      @Override
      public void onSuccess(final MemcacheStatus result) {
        setSuccesses.mark();
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        setFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureIncrDecrFuture(ListenableFuture<Long> future) {
    final TimerContext ctx = incrDecrs.time();

    final FutureCallback<Long> metricsCallback = new FutureCallback<Long>() {
      @Override
      public void onSuccess(final Long result) {
        incrDecrSuccesses.mark();
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        incrDecrFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void measureTouchFuture(ListenableFuture<MemcacheStatus> future) {
    final TimerContext ctx = touches.time();

    final FutureCallback<MemcacheStatus> metricsCallback = new FutureCallback<MemcacheStatus>() {
      @Override
      public void onSuccess(final MemcacheStatus result) {
        touchSuccesses.mark();
        ctx.stop();
      }

      @Override
      public void onFailure(final Throwable t) {
        touchFailures.mark();
        ctx.stop();
      }
    };
    Futures.addCallback(future, metricsCallback, Utils.SAME_THREAD_EXECUTOR);
  }

  @Override
  public void registerOutstandingRequestsGauge(OutstandingRequestsGauge gauge) {
    this.internalOutstandingReqGauge = gauge;
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

  public Meter getGetSuccesses() {
    return getSuccesses;
  }

  public Meter getGetFailures() {
    return getFailures;
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

  public Timer getSets() {
    return sets;
  }

  public Meter getSetSuccesses() {
    return setSuccesses;
  }

  public Meter getSetFailures() {
    return setFailures;
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

  public Gauge<Integer> getOutstandingRequestsGauge() {
    return outstandingRequestsGauge;
  }
}

package com.spotify.folsom.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import com.codahale.metrics.Gauge;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics.OutstandingRequestsGauge;
import com.spotify.futures.CompletableFutures;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

import java.util.concurrent.CompletableFuture;
import org.junit.Test;

import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SemanticFolsomMetricsTest {

  private final OutstandingRequestsGauge GAUGE = () -> 123;

  private final SemanticMetricRegistry registry = new SemanticMetricRegistry();
  private final SemanticFolsomMetrics metrics =
      new SemanticFolsomMetrics(registry, MetricId.EMPTY);

  @Test
  public void testGetMiss() {
    metrics.measureGetFuture(CompletableFuture.completedFuture(null));

    assertEquals(1, metrics.getGets().getCount());
    assertEquals(1, metrics.getGetMisses().getCount());
    assertEquals(0, metrics.getGetHits().getCount());
    assertEquals(0, metrics.getGetFailures().getCount());
  }

  @Test
  public void testGetHit() {
    metrics.measureGetFuture(CompletableFuture.completedFuture(GetResult.success(new byte[]{}, 0)));

    assertEquals(1, metrics.getGets().getCount());
    assertEquals(0, metrics.getGetMisses().getCount());
    assertEquals(1, metrics.getGetHits().getCount());
    assertEquals(0, metrics.getGetFailures().getCount());
  }

  @Test
  public void testGetFailure() {
    metrics.measureGetFuture(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()));

    assertEquals(1, metrics.getGets().getCount());
    assertEquals(0, metrics.getGetMisses().getCount());
    assertEquals(0, metrics.getGetHits().getCount());
    assertEquals(1, metrics.getGetFailures().getCount());
  }


  @Test
  public void testSetSuccess() {
    metrics.measureSetFuture(CompletableFuture.completedFuture(MemcacheStatus.OK));

    assertEquals(1, metrics.getSets().getCount());
    assertEquals(1, metrics.getSetSuccesses().getCount());
    assertEquals(0, metrics.getSetFailures().getCount());
  }

  @Test
  public void testSetFailure() {
    metrics.measureSetFuture(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()));

    assertEquals(1, metrics.getSets().getCount());
    assertEquals(0, metrics.getSetSuccesses().getCount());
    assertEquals(1, metrics.getSetFailures().getCount());
  }

  @Test
  public void testMultigetSuccess() {
    metrics.measureMultigetFuture(CompletableFuture.completedFuture(
        ImmutableList.of(
            GetResult.success(new byte[]{1}, 0),
            GetResult.success(new byte[]{1}, 0)
        )
    ));

    assertEquals(1, metrics.getMultigets().getCount());
    assertEquals(1, metrics.getMultigetSuccesses().getCount());
    assertEquals(0, metrics.getMultigetFailures().getCount());
    assertEquals(2, metrics.getGetHits().getCount());
    assertEquals(0, metrics.getGetMisses().getCount());
  }

  @Test
  public void testMultigetFailure() {
    metrics.measureMultigetFuture(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()));

    assertEquals(1, metrics.getMultigets().getCount());
    assertEquals(0, metrics.getMultigetSuccesses().getCount());
    assertEquals(1, metrics.getMultigetFailures().getCount());
  }

  @Test
  public void testDeleteSuccess() {
    metrics.measureDeleteFuture(CompletableFuture.completedFuture(MemcacheStatus.OK));

    assertEquals(1, metrics.getDeletes().getCount());
    assertEquals(1, metrics.getDeleteSuccesses().getCount());
    assertEquals(0, metrics.getDeleteFailures().getCount());
  }

  @Test
  public void testDeleteFailure() {
    metrics.measureDeleteFuture(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()));

    assertEquals(1, metrics.getDeletes().getCount());
    assertEquals(0, metrics.getDeleteSuccesses().getCount());
    assertEquals(1, metrics.getDeleteFailures().getCount());
  }

  @Test
  public void testIncrDecrSuccess() {
    metrics.measureIncrDecrFuture(CompletableFuture.completedFuture(1L));

    assertEquals(1, metrics.getIncrDecrs().getCount());
    assertEquals(1, metrics.getIncrDecrSuccesses().getCount());
    assertEquals(0, metrics.getIncrDecrFailures().getCount());
  }

  @Test
  public void testIncrDecrFailure() {
    metrics.measureIncrDecrFuture(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()));

    assertEquals(1, metrics.getIncrDecrs().getCount());
    assertEquals(0, metrics.getIncrDecrSuccesses().getCount());
    assertEquals(1, metrics.getIncrDecrFailures().getCount());
  }

  @Test
  public void testTouchSuccess() {
    metrics.measureTouchFuture(CompletableFuture.completedFuture(MemcacheStatus.OK));

    assertEquals(1, metrics.getTouches().getCount());
    assertEquals(1, metrics.getTouchSuccesses().getCount());
    assertEquals(0, metrics.getTouchFailures().getCount());
  }

  @Test
  public void testTouchFailure() {
    metrics.measureTouchFuture(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()));

    assertEquals(1, metrics.getTouches().getCount());
    assertEquals(0, metrics.getTouchSuccesses().getCount());
    assertEquals(1, metrics.getTouchFailures().getCount());
  }

  @Test
  public void testRegisterOutstandingRequestsGauge() {
    metrics.registerOutstandingRequestsGauge(GAUGE);
    @SuppressWarnings("rawtypes")
    final SortedMap<MetricId, Gauge> gauges = registry.getGauges(
        (name, metric) -> "outstanding-requests".equals(name.getTags().get("what")));
    assertEquals(1, gauges.size());
    assertEquals(123, gauges.values().iterator().next().getValue());
  }

  @Test
  public void testRegisterOutstandingRequestsGaugeDuplicate() {
    metrics.registerOutstandingRequestsGauge(GAUGE);
    metrics.registerOutstandingRequestsGauge(GAUGE);
 }

  @Test
  public void testGetAndMultigetHitRatio() {
    metrics.measureGetFuture(CompletableFuture.completedFuture(GetResult.success(new byte[]{1}, 1)));
    metrics.measureGetFuture(CompletableFuture.completedFuture(missResult()));
    metrics.measureGetFuture(CompletableFuture.completedFuture(GetResult.success(new byte[]{2}, 1)));

    final List<GetResult<byte[]>> list = Lists.newArrayList(
        GetResult.success(new byte[]{3}, 3),
        GetResult.success(new byte[]{4}, 4),
        missResult()
    );
    metrics.measureMultigetFuture(CompletableFuture.completedFuture(list));

    assertEquals(4, metrics.getGetHits().getCount());
    assertEquals(2, metrics.getGetMisses().getCount());

    // the Meter implementation will not "tick" unless 5 seconds has passed since data was recorded;
    // so without any pause the meters used in the ratio will return a 5-minute-rate of 0.0.
    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

    // four hits out of six total attempts at fetching entries
    assertEquals(4.0 / 6.0, metrics.getHitRatio().getValue(), 0.1);
  }

  private GetResult<byte[]> missResult() {
    return null;
  }

}
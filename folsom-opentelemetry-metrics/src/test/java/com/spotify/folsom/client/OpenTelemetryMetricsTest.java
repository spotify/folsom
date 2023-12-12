/*
 * Copyright (c) 2015 Spotify AB
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

import static org.junit.Assert.assertEquals;

import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.data.*;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class OpenTelemetryMetricsTest {

  public static final AttributeKey<String> RESULT_TAG = AttributeKey.stringKey("result");
  public static final AttributeKey<String> OPERATION_TAG = AttributeKey.stringKey("operation");

  public static final String MEMCACHE_REQUESTS_SET = "memcache.requests.set";
  public static final String MEMCACHE_REQUESTS_GET = "memcache.requests.get";
  public static final String MEMCACHE_REQUESTS_MULTIGET = "memcache.requests.multiget";
  public static final String MEMCACHE_REQUESTS_DELETE = "memcache.requests.delete";
  public static final String MEMCACHE_REQUESTS_INCR_DECR = "memcache.requests.increment_decrement";
  public static final String MEMCACHE_REQUESTS_TOUCH = "memcache.requests.touch";
  public static final String MEMCACHE_REQUESTS_NANOSECONDS = "memcache.requests.nanoseconds";

  public static final GetResult<byte[]> SUCCESS = GetResult.success(new byte[] {}, 0L);
  public static final RuntimeException FAILURE = new RuntimeException("ex");
  public static final GetResult<byte[]> MISS = null;
  public static final double EPS = 0.1;

  @Rule public OpenTelemetryRule otelTesting = OpenTelemetryRule.create();

  private OpenTelemetryMetrics metrics;

  @Before
  public void setUp() {
    OpenTelemetry openTelemetry = otelTesting.getOpenTelemetry();
    metrics = new OpenTelemetryMetrics(openTelemetry);
  }

  @Test
  public void shouldRecordCacheGetMiss() {
    // given
    CompletionStage<GetResult<byte[]>> future = CompletableFuture.completedFuture(MISS);

    // when
    metrics.measureGetFuture(future);

    // then
    LongPointData counterData =
        findMetric(MEMCACHE_REQUESTS_GET, RESULT_TAG.getKey(), "miss", LongPointData.class);
    HistogramPointData histData =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS, OPERATION_TAG.getKey(), "get", HistogramPointData.class);

    assertEquals(1, counterData.getValue());
    assertEquals(1, histData.getCount());
  }

  @Test
  public void shouldRecordCacheGetHit() {
    // given
    CompletionStage<GetResult<byte[]>> future = CompletableFuture.completedFuture(SUCCESS);

    // when
    metrics.measureGetFuture(future);

    // then
    LongPointData hit =
        findMetric(MEMCACHE_REQUESTS_GET, RESULT_TAG.getKey(), "hit", LongPointData.class);
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS, OPERATION_TAG.getKey(), "get", HistogramPointData.class);

    assertEquals(1, hit.getValue());
    assertEquals(1, duration.getCount());
  }

  @Test
  public void shouldRecordCacheGetFailure() {
    // given
    CompletableFuture<GetResult<byte[]>> future = new CompletableFuture<>();
    future.completeExceptionally(FAILURE);

    // when
    metrics.measureGetFuture(future);

    // then
    LongPointData failure =
        findMetric(MEMCACHE_REQUESTS_GET, RESULT_TAG.getKey(), "failure", LongPointData.class);
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS, OPERATION_TAG.getKey(), "get", HistogramPointData.class);

    assertEquals(1, failure.getValue());
    assertEquals(1, duration.getCount());
  }

  @Test
  public void shouldRecordCacheGetFailureAndOtherResult() {
    // given
    CompletableFuture<GetResult<byte[]>> failureFuture = new CompletableFuture<>();
    failureFuture.completeExceptionally(FAILURE);
    CompletionStage<GetResult<byte[]>> successFuture = CompletableFuture.completedFuture(SUCCESS);

    // when
    metrics.measureGetFuture(failureFuture);
    metrics.measureGetFuture(successFuture);

    // when
    LongPointData failure =
        findMetric(MEMCACHE_REQUESTS_GET, "result", "failure", LongPointData.class);
    LongPointData hit = findMetric(MEMCACHE_REQUESTS_GET, "result", "hit", LongPointData.class);
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS, OPERATION_TAG.getKey(), "get", HistogramPointData.class);

    assertEquals(1, hit.getValue());
    assertEquals(1, failure.getValue());
    assertEquals(2, duration.getCount());
  }

  @Test
  public void shouldRecordCacheMultiGetMissWhenOnlyMissed() {
    // given
    final int expected = 5;
    List<GetResult<byte[]>> results = new ArrayList<>();
    for (int i = 0; i < expected; i++) {
      results.add(MISS);
    }
    CompletionStage<List<GetResult<byte[]>>> future = CompletableFuture.completedFuture(results);

    // when
    metrics.measureMultigetFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "multiget",
            HistogramPointData.class);
    LongPointData miss =
        findMetric(MEMCACHE_REQUESTS_MULTIGET, "result", "miss", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(expected, miss.getValue());
  }

  @Test
  public void shouldRecordCacheMultiGetMissWhenMissedAndHit() {
    // given
    Random random = new Random();
    int expectedHits = 0, expectedMisses = 0;
    List<GetResult<byte[]>> results = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      int randNum = random.nextInt(6);
      if (randNum % 2 == 0) {
        results.add(SUCCESS);
        expectedHits++;
      } else {
        results.add(MISS);
        expectedMisses++;
      }
    }
    CompletionStage<List<GetResult<byte[]>>> future = CompletableFuture.completedFuture(results);

    // when
    metrics.measureMultigetFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "multiget",
            HistogramPointData.class);
    LongPointData miss =
        findMetric(MEMCACHE_REQUESTS_MULTIGET, "result", "miss", LongPointData.class);
    LongPointData hit =
        findMetric(MEMCACHE_REQUESTS_MULTIGET, "result", "hit", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(expectedMisses, miss.getValue());
    assertEquals(expectedHits, hit.getValue());
  }

  @Test
  public void shouldRecordCacheMultiGetHitWhenOnlyHit() {
    // given
    final int expected = 5;
    List<GetResult<byte[]>> results = new ArrayList<>();
    for (int i = 0; i < expected; i++) {
      results.add(SUCCESS);
    }
    CompletionStage<List<GetResult<byte[]>>> future = CompletableFuture.completedFuture(results);

    // when
    metrics.measureMultigetFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "multiget",
            HistogramPointData.class);
    LongPointData miss =
        findMetric(MEMCACHE_REQUESTS_MULTIGET, "result", "hit", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(expected, miss.getValue());
  }

  @Test
  public void shouldRecordCacheMultiGetFailure() {
    // given
    CompletableFuture<List<GetResult<byte[]>>> future = new CompletableFuture<>();
    future.completeExceptionally(FAILURE);

    // when
    metrics.measureMultigetFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "multiget",
            HistogramPointData.class);
    LongPointData failures =
        findMetric(MEMCACHE_REQUESTS_MULTIGET, "result", "failure", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(1, failures.getValue());
  }

  @Test
  public void shouldRecordCacheDeleteSuccess() {
    // given
    CompletionStage<MemcacheStatus> future = CompletableFuture.completedFuture(MemcacheStatus.OK);

    // when
    metrics.measureDeleteFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "delete",
            HistogramPointData.class);
    LongPointData successes =
        findMetric(MEMCACHE_REQUESTS_DELETE, "result", "success", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(1, successes.getValue());
  }

  @Test
  public void shouldRecordCacheDeleteFailure() {
    // given
    CompletableFuture<MemcacheStatus> future = new CompletableFuture<>();
    future.completeExceptionally(FAILURE);

    // when
    metrics.measureDeleteFuture(future);
    LongPointData failures =
        findMetric(MEMCACHE_REQUESTS_DELETE, "result", "failure", LongPointData.class);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "delete",
            HistogramPointData.class);
    assertEquals(1, duration.getCount());
    assertEquals(1, failures.getValue());
  }

  @Test
  public void shouldRecordCacheDeleteFailureAndSuccess() {
    // given
    CompletableFuture<MemcacheStatus> failureFuture = new CompletableFuture<>();
    failureFuture.completeExceptionally(FAILURE);
    CompletionStage<MemcacheStatus> successFuture =
        CompletableFuture.completedFuture(MemcacheStatus.OK);

    // when
    metrics.measureDeleteFuture(failureFuture);
    metrics.measureDeleteFuture(successFuture);
    LongPointData failures =
        findMetric(MEMCACHE_REQUESTS_DELETE, "result", "failure", LongPointData.class);
    LongPointData successes =
        findMetric(MEMCACHE_REQUESTS_DELETE, "result", "success", LongPointData.class);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "delete",
            HistogramPointData.class);
    assertEquals(duration.getCount(), 2);
    assertEquals(1, failures.getValue());
    assertEquals(1, successes.getValue());
  }

  @Test
  public void shouldRecordCacheSetSuccess() {
    // given
    CompletionStage<MemcacheStatus> future = CompletableFuture.completedFuture(MemcacheStatus.OK);

    // when
    metrics.measureSetFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS, OPERATION_TAG.getKey(), "set", HistogramPointData.class);
    LongPointData successes =
        findMetric(MEMCACHE_REQUESTS_SET, "result", "success", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(1, successes.getValue());
  }

  @Test
  public void shouldRecordCacheSetFailure() {
    // given
    CompletableFuture<MemcacheStatus> failureFuture = new CompletableFuture<>();
    failureFuture.completeExceptionally(FAILURE);

    // when
    metrics.measureSetFuture(failureFuture);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS, OPERATION_TAG.getKey(), "set", HistogramPointData.class);
    LongPointData failures =
        findMetric(MEMCACHE_REQUESTS_SET, "result", "failure", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(1, failures.getValue());
  }

  @Test
  public void shouldRecordCacheSetFailureAndHit() {
    // given
    CompletableFuture<MemcacheStatus> failureFuture = new CompletableFuture<>();
    failureFuture.completeExceptionally(FAILURE);
    CompletionStage<MemcacheStatus> successFuture =
        CompletableFuture.completedFuture(MemcacheStatus.OK);

    // when
    metrics.measureSetFuture(failureFuture);
    metrics.measureSetFuture(successFuture);

    // then
    LongPointData failures =
        findMetric(MEMCACHE_REQUESTS_SET, "result", "failure", LongPointData.class);
    LongPointData successes =
        findMetric(MEMCACHE_REQUESTS_SET, "result", "success", LongPointData.class);
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS, OPERATION_TAG.getKey(), "set", HistogramPointData.class);

    assertEquals(2, duration.getCount());
    assertEquals(1, failures.getValue());
    assertEquals(1, successes.getValue());
  }

  @Test
  public void shouldRecordCacheIncrDecrSuccess() {
    // given
    CompletableFuture<Long> future = CompletableFuture.completedFuture(1L);

    // when
    metrics.measureIncrDecrFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "increment_decrement",
            HistogramPointData.class);
    LongPointData successes =
        findMetric(MEMCACHE_REQUESTS_INCR_DECR, "result", "success", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(1, successes.getValue());
  }

  @Test
  public void shouldRecordCacheIncrDecrFailure() {
    // given
    CompletableFuture<Long> future = new CompletableFuture<>();
    future.completeExceptionally(FAILURE);

    // when
    metrics.measureIncrDecrFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "increment_decrement",
            HistogramPointData.class);
    LongPointData failure =
        findMetric(MEMCACHE_REQUESTS_INCR_DECR, "result", "failure", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(1, failure.getValue());
  }

  @Test
  public void shouldRecordCacheIncrDecrFailureAndSuccess() {
    // given
    CompletableFuture<Long> failureFuture = new CompletableFuture<>();
    CompletableFuture<Long> successFuture = CompletableFuture.completedFuture(1L);
    failureFuture.completeExceptionally(FAILURE);

    // when
    metrics.measureIncrDecrFuture(failureFuture);
    metrics.measureIncrDecrFuture(successFuture);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "increment_decrement",
            HistogramPointData.class);
    LongPointData failure =
        findMetric(MEMCACHE_REQUESTS_INCR_DECR, "result", "failure", LongPointData.class);
    LongPointData successes =
        findMetric(MEMCACHE_REQUESTS_INCR_DECR, "result", "success", LongPointData.class);

    assertEquals(2, duration.getCount());
    assertEquals(1, failure.getValue());
    assertEquals(1, successes.getValue());
  }

  @Test
  public void shouldRecordCacheTouchSuccess() {
    // given
    CompletionStage<MemcacheStatus> future = CompletableFuture.completedFuture(MemcacheStatus.OK);

    // when
    metrics.measureTouchFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "touch",
            HistogramPointData.class);
    LongPointData successes =
        findMetric(MEMCACHE_REQUESTS_TOUCH, "result", "success", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(1, successes.getValue());
  }

  @Test
  public void shouldRecordCacheTouchFailure() {
    // given
    CompletableFuture<MemcacheStatus> future = new CompletableFuture<>();
    future.completeExceptionally(FAILURE);

    // when
    metrics.measureTouchFuture(future);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "touch",
            HistogramPointData.class);
    LongPointData failure =
        findMetric(MEMCACHE_REQUESTS_TOUCH, "result", "failure", LongPointData.class);

    assertEquals(1, duration.getCount());
    assertEquals(1, failure.getValue());
  }

  @Test
  public void shouldRecordCacheTouchFailureAndSuccess() {
    // given
    CompletableFuture<MemcacheStatus> failureFuture = new CompletableFuture<>();
    CompletableFuture<MemcacheStatus> successFuture =
        CompletableFuture.completedFuture(MemcacheStatus.OK);
    failureFuture.completeExceptionally(FAILURE);

    // when
    metrics.measureTouchFuture(failureFuture);
    metrics.measureTouchFuture(successFuture);

    // then
    HistogramPointData duration =
        findMetric(
            MEMCACHE_REQUESTS_NANOSECONDS,
            OPERATION_TAG.getKey(),
            "touch",
            HistogramPointData.class);
    LongPointData failure =
        findMetric(MEMCACHE_REQUESTS_TOUCH, "result", "failure", LongPointData.class);
    LongPointData successes =
        findMetric(MEMCACHE_REQUESTS_TOUCH, "result", "success", LongPointData.class);

    assertEquals(2, duration.getCount());
    assertEquals(1, failure.getValue());
    assertEquals(1, successes.getValue());
  }

  @Test
  public void shouldRegisterOutstandingRequests() {
    // given
    Metrics.OutstandingRequestsGauge gauge1 = () -> 123;
    Metrics.OutstandingRequestsGauge gauge2 = () -> 321;
    Metrics.OutstandingRequestsGauge gauge3 = () -> 0;

    // when
    metrics.registerOutstandingRequestsGauge(gauge1);
    metrics.registerOutstandingRequestsGauge(gauge2);
    metrics.registerOutstandingRequestsGauge(gauge3);

    // then
    GaugeData<DoublePointData> gaugeData = getGaugeByName("memcache.outstanding.requests");
    DoublePointData doublePointData = new ArrayList<>(gaugeData.getPoints()).get(0);

    assertEquals(444.0, doublePointData.getValue(), EPS);
  }

  @Test
  public void shouldUnregisterOutstandingRequests() {
    // given
    Metrics.OutstandingRequestsGauge gauge1 = () -> 123;
    Metrics.OutstandingRequestsGauge gauge2 = () -> 321;
    Metrics.OutstandingRequestsGauge gauge3 = () -> 0;

    // when
    metrics.registerOutstandingRequestsGauge(gauge1);
    metrics.registerOutstandingRequestsGauge(gauge2);
    metrics.registerOutstandingRequestsGauge(gauge3);

    metrics.unregisterOutstandingRequestsGauge(gauge1);
    metrics.unregisterOutstandingRequestsGauge(gauge3);

    // then
    GaugeData<DoublePointData> gaugeData = getGaugeByName("memcache.outstanding.requests");
    DoublePointData doublePointData = new ArrayList<>(gaugeData.getPoints()).get(0);

    assertEquals(321.0, doublePointData.getValue(), EPS);
  }

  @Test
  public void shouldRegisterGaugesWithoutRaceCondition() throws InterruptedException {
    // given
    int numThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);

    // when
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            for (int j = 1; j <= 999; j++) {
              int outstandingRequests = j;
              metrics.registerOutstandingRequestsGauge(() -> outstandingRequests);
            }
            latch.countDown();
          });
    }

    latch.await();
    executorService.shutdown();

    // then
    GaugeData<DoublePointData> gaugeData = getGaugeByName("memcache.outstanding.requests");
    DoublePointData doublePointData = new ArrayList<>(gaugeData.getPoints()).get(0);
    double nth_partial_sum =
        (double) (999 * 1000)
            / 2; // https://en.wikipedia.org/wiki/1_%2B_2_%2B_3_%2B_4_%2B_%E2%8B%AF
    double sum = nth_partial_sum * numThreads;

    assertEquals(sum, doublePointData.getValue(), EPS);
  }

  private GaugeData<DoublePointData> getGaugeByName(String gaugeName) {
    return otelTesting.getMetrics().stream()
        .filter(metricData -> Objects.equals(gaugeName, metricData.getName()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("gauge not found"))
        .getDoubleGaugeData();
  }

  private <T extends PointData> T findMetric(
      String metricName, String key, String value, Class<T> targetClass) {
    PointData pointData =
        otelTesting.getMetrics().stream()
            .filter(metricData -> Objects.equals(metricName, metricData.getName()))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("metric not found"))
            .getData()
            .getPoints()
            .stream()
            .filter(
                pd -> Objects.equals(value, pd.getAttributes().get(AttributeKey.stringKey(key))))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("attr not found"));

    if (!targetClass.isInstance(pointData)) {
      throw new IllegalArgumentException("can't cast to " + targetClass.getName());
    }

    return targetClass.cast(pointData);
  }
}

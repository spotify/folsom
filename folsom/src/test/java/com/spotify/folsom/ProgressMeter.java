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
/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.folsom;

import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLong;

public class ProgressMeter {

  static class Delta {

    Delta(final long ops, final long time, final long latency) {
      this.ops = ops;
      this.time = time;
      this.latency = latency;
    }

    public final long ops;
    public final long time;
    public final long latency;
  }

  private long lastRows = 0;
  private long lastTime = System.nanoTime();
  private long lastLatency = 0;
  private final long interval = 1000;

  private final String unit;

  private final AtomicLong latency = new AtomicLong();
  private final AtomicLong operations = new AtomicLong();

  private final ArrayDeque<Delta> deltas = new ArrayDeque<>();

  private volatile boolean run = true;

  private final Thread worker;

  public ProgressMeter() {
    this("ops");
  }

  public ProgressMeter(final String unit) {
    this.unit = unit;
    worker = new Thread(() -> {
      while (run) {
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          continue;
        }
        progress();
      }
    });
    worker.start();
  }

  private void progress() {
    final long count = this.operations.get();
    final long time = System.nanoTime();
    final long latency = this.latency.get();

    final long delta = count - lastRows;
    final long deltaTime = time - lastTime;
    final long deltaLatency = latency - lastLatency;

    deltas.add(new Delta(delta, deltaTime, deltaLatency));

    if (deltas.size() > 10) {
      deltas.pop();
    }

    long opSum = 0;
    long timeSum = 0;
    long latencySum = 0;

    for (final Delta d : deltas) {
      timeSum += d.time;
      opSum += d.ops;
      latencySum += d.latency;
    }

    final long operations = deltaTime == 0 ? 0 : 1000000000 * delta / deltaTime;
    final long averagedOperations = timeSum == 0 ? 0 : 1000000000 * opSum / timeSum;
    final double averageLatency = opSum == 0 ? 0 : latencySum / (1000000.d * opSum);

    com.sun.management.OperatingSystemMXBean operatingSystemMXBean =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    double totalCPU = 100.0 * operatingSystemMXBean.getSystemCpuLoad();
    double processCPU = 100.0 * operatingSystemMXBean.getProcessCpuLoad();
    System.out.printf(
        "%,10d (%,10d) %s/s. %,10.9f ms average latency. "
        + "%,10d %s total. process=%3.0f%%, total=%3.0f%%\n",
        operations,
        averagedOperations,
        unit,
        averageLatency,
        count,
        unit,
        processCPU,
        totalCPU);
    System.out.flush();

    lastRows = count;
    lastTime = time;
    lastLatency = latency;
  }

  public void finish() {
    run = false;
    worker.interrupt();
    try {
      worker.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    progress();
  }

  public void inc(final long ops, final long latency) {
    this.operations.addAndGet(ops);
    this.latency.addAndGet(latency);
  }
}

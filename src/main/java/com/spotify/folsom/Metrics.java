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

package com.spotify.folsom;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface Metrics {
  void measureGetFuture(ListenableFuture<GetResult<byte[]>> future);
  void measureMultigetFuture(ListenableFuture<List<GetResult<byte[]>>> future);
  void measureDeleteFuture(ListenableFuture<MemcacheStatus> future);
  void measureSetFuture(ListenableFuture<MemcacheStatus> future);
  void measureIncrDecrFuture(ListenableFuture<Long> future);
  void measureTouchFuture(ListenableFuture<MemcacheStatus> future);

  /**
   * Called by the MemcacheClient initialization process to allow a gauge to be registered with the
   * metrics implementation to monitor the number of outstanding requests at any moment in time.
   */
  void registerOutstandingRequestsGauge(OutstandingRequestsGauge gauge);

  interface OutstandingRequestsGauge {
    int getOutstandingRequests();
  }

}

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

import static org.mockito.Mockito.verify;

import com.spotify.folsom.Span;
import io.opencensus.trace.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OpenCensusSpanTest {

  @Mock private io.opencensus.trace.Span wrapped;

  @Test
  public void succeed() {
    final Span span = new OpenCensusSpan(wrapped, true);
    span.success();
    span.close();

    verify(wrapped).setStatus(Status.OK);
    verify(wrapped).end();
  }

  @Test
  public void fail() {
    final Span span = new OpenCensusSpan(wrapped, true);
    span.failure();
    span.close();

    verify(wrapped).setStatus(Status.UNKNOWN);
    verify(wrapped).end();
  }
}

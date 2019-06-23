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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.google.common.io.BaseEncoding;
import com.spotify.folsom.Span;
import io.opencensus.trace.Status;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OpenCensusSpanTest {

  private static final BaseEncoding HEX = BaseEncoding.base16().lowerCase();
  private static final byte[] VALUE = "value".getBytes(StandardCharsets.US_ASCII);

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

  @Test
  public void includedValue() {
    final Span span = new OpenCensusSpan(wrapped, true);
    span.value(VALUE);

    verify(wrapped).putAttribute("value_size_bytes", longAttributeValue(VALUE.length));
    verify(wrapped).putAttribute("value_hex", stringAttributeValue(HEX.encode(VALUE)));
  }

  @Test
  public void nonIncludedValue() {
    final Span span = new OpenCensusSpan(wrapped, false);
    span.value(VALUE);

    verify(wrapped).putAttribute("value_size_bytes", longAttributeValue(VALUE.length));
    verify(wrapped, never()).putAttribute("value_hex", stringAttributeValue(HEX.encode(VALUE)));
  }

  @Test
  public void nullValue() {
    final Span span = new OpenCensusSpan(wrapped, true);
    span.value(null);

    verifyZeroInteractions(wrapped);
  }
}

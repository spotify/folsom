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
import static java.util.Objects.requireNonNull;

import com.google.common.io.BaseEncoding;
import com.spotify.folsom.Span;
import io.opencensus.trace.Status;

class OpenCensusSpan implements Span {

  private static final BaseEncoding HEX = BaseEncoding.base16().lowerCase();

  private final io.opencensus.trace.Span span;
  private final boolean includeValues;

  OpenCensusSpan(final io.opencensus.trace.Span span, final boolean includeValues) {
    this.span = requireNonNull(span);
    this.includeValues = includeValues;
  }

  @Override
  public Span value(final byte[] value) {
    if (value != null) {
      if (includeValues) {
        span.putAttribute("value_hex", stringAttributeValue(HEX.encode(value)));
      }
      span.putAttribute("value_size_bytes", longAttributeValue(value.length));
    }
    return this;
  }

  @Override
  public Span success() {
    span.setStatus(Status.OK);
    return this;
  }

  @Override
  public Span failure() {
    span.setStatus(Status.UNKNOWN);
    return this;
  }

  @Override
  public void close() {
    span.end();
  }
}

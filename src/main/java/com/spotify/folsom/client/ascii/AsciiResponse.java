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
package com.spotify.folsom.client.ascii;

public class AsciiResponse {
  public static final AsciiResponse STORED = new AsciiResponse(Type.STORED);
  public static final AsciiResponse EXISTS = new AsciiResponse(Type.EXISTS);
  public static final AsciiResponse DELETED = new AsciiResponse(Type.DELETED);
  public static final AsciiResponse NOT_FOUND = new AsciiResponse(Type.NOT_FOUND);
  public static final AsciiResponse NOT_STORED = new AsciiResponse(Type.NOT_STORED);
  public static final AsciiResponse TOUCHED = new AsciiResponse(Type.TOUCHED);

  public final Type type;

  protected AsciiResponse(Type type) {
    this.type = type;
  }

  public enum Type {
    VALUE, NUMERIC_VALUE, STORED, EXISTS, DELETED, NOT_FOUND, NOT_STORED, TOUCHED
  }
}

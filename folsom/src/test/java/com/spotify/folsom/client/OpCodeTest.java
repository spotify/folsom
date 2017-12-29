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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OpCodeTest {

  @Test
  public void testGetKind() throws Exception {
    assertEquals(OpCode.SET, OpCode.getKind(OpCode.SET));
    assertEquals(OpCode.SET, OpCode.getKind(OpCode.APPEND));
    assertEquals(OpCode.SET, OpCode.getKind(OpCode.REPLACE));
    assertEquals(OpCode.SET, OpCode.getKind(OpCode.ADD));
    assertEquals(OpCode.SET, OpCode.getKind(OpCode.DELETE));

    assertEquals(OpCode.GET, OpCode.getKind(OpCode.GET));
    assertEquals(OpCode.GET, OpCode.getKind(OpCode.GETQ));

    assertEquals(OpCode.TOUCH, OpCode.getKind(OpCode.TOUCH));
 }
}

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

import java.util.EnumSet;

public enum OpCode {
  GET(0x00),
  SET(0x01),
  ADD(0x02),
  REPLACE(0x03),
  DELETE(0x04),
  INCREMENT(0x05),
  DECREMENT(0x06),
  QUIT(7),
  FLUSH(0x08),
  GETQ(0x09),
  NOOP(0x0a),

  VERSION(0x0b),
  GETK(0x0c),
  GETKQ(0x0d),
  APPEND(0x0e),
  PREPEND(0x0f),
  STAT(0x10),
  SETQ(0x11),
  ADDQ(0x12),
  REPLACEQ(0x13),
  DELETEQ(0x14),
  INCREMENTQ(0x15),
  DECREMENTQ(0x16),
  QUITQ(0x17),
  FLUSHQ(0x18),

  APPENDQ(0x19),
  PREPANDQ(0x1a),
  VERBOSITY(0x1b),
  TOUCH(0x1c),
  GAT(0x1d),
  GATQ(0x1e),
  SASL_LIST_MECHS(0x20),
  SASL_AUTH(0x21),
  SASL_STEP(0x22),

  RGET(0x30),
  RSET(0x31),
  RSETQ(0x32),
  RAPPEND(0x33),
  RAPPENDQ(0x34),
  RPREPEND(0x35),
  RPREPENDQ(0x36),
  RDELETE(0x37),
  RDELETEQ(0x38),
  RINCR(0x39),
  RINCRQ(0x3a),
  RDECR(0x3b),
  RDECRQ(0x3c),

  SET_VBUCKET(0x3d),
  GET_VBUCKET(0x3e),
  DEL_VBUCKET(0x3f),

  TAP_MUTATION(0x41),
  TAP_DELETE(0x42),
  TAP_FLUSH(0x43),
  TAP_OPAQUE(0x44),
  TAP_VBUCKET_SET(0x45),
  TAP_CHECKPOINT_START(0x46),
  TAP_CHECKPOINT_END(0x47);

  // quick lookup by value
  private static final OpCode[] BY_VALUE = new OpCode[TAP_CHECKPOINT_END.opcode + 1];

  static {
    final EnumSet<OpCode> all = EnumSet.allOf(OpCode.class);
    all.stream().forEach(opCode -> BY_VALUE[opCode.opcode] = opCode);
  }

  public static OpCode of(final byte value) {
    if (value >= 0 && value < BY_VALUE.length) {
      final OpCode opCode = BY_VALUE[value];
      if (opCode != null) {
        return opCode;
      }
    }

    throw new IllegalArgumentException("Unknown opcode: " + value);
  }

  public static OpCode getKind(final OpCode opcode) {
    switch (opcode) {
      case GET:
      case GETQ:
      case GAT:
        return GET;
      case SET:
      case APPEND:
      case PREPEND:
      case ADD:
      case REPLACE:
      case DELETE:
        return SET;
      default:
        return opcode;
    }
  }

  private final byte opcode;

  OpCode(final int opcode) {
    this.opcode = (byte) opcode;
  }

  public byte value() {
    return opcode;
  }
}

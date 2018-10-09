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


public final class OpCode {

  private OpCode() {
  }

  public static final byte GET = 0x00;
  public static final byte SET = 0x01;
  public static final byte ADD = 0x02;
  public static final byte REPLACE = 0x03;
  public static final byte DELETE = 0x04;
  public static final byte INCREMENT = 0x05;
  public static final byte DECREMENT = 0x06;
  public static final int QUIT = 7;
  public static final byte FLUSH = 0x08;
  public static final byte GETQ = 0x09;
  public static final byte NOOP = 0x0a;

  public static final int VERSION = 0x0b;
  public static final int GETK = 0x0c;
  public static final int GETKQ = 0x0d;
  public static final byte APPEND = 0x0e;
  public static final byte PREPEND = 0x0f;
  public static final int STAT = 0x10;
  public static final int SETQ = 0x11;
  public static final int ADDQ = 0x12;
  public static final int REPLACEQ = 0x13;
  public static final int DELETEQ = 0x14;
  public static final int INCREMENTQ = 0x15;
  public static final int DECREMENTQ = 0x16;
  public static final int QUITQ = 0x17;
  public static final int FLUSHQ = 0x18;

  public static final int APPENDQ = 0x19;
  public static final int PREPANDQ = 0x1a;
  public static final int VERBOSITY = 0x1b;
  public static final byte TOUCH = 0x1c;
  public static final int GAT = 0x1d;
  public static final int GATQ = 0x1e;
  public static final int SASL_LIST_MECHS = 0x20;
  public static final byte SASL_AUTH = 0x21;
  public static final int SASL_STEP = 0x22;

  public static final int RGET = 0x30;
  public static final int RSET = 0x31;
  public static final int RSETQ = 0x32;
  public static final int RAPPEND = 0x33;
  public static final int RAPPENDQ = 0x34;
  public static final int RPREPEND = 0x35;
  public static final int RPREPENDQ = 0x36;
  public static final int RDELETE = 0x37;
  public static final int RDELETEQ = 0x38;
  public static final int RINCR = 0x39;
  public static final int RINCRQ = 0x3a;
  public static final int RDECR = 0x3b;
  public static final int RDECRQ = 0x3c;

  public static final int SET_VBUCKET = 0x3d;
  public static final int GET_VBUCKET = 0x3e;
  public static final int DEL_VBUCKET = 0x3f;

  public static final int TAP_CONNECT = 0x40;
  public static final int TAP_MUTATION = 0x41;
  public static final int TAP_DELETE = 0x42;
  public static final int TAP_FLUSH = 0x43;
  public static final int TAP_OPAQUE = 0x44;
  public static final int TAP_VBUCKET_SET = 0x45;
  public static final int TAP_CHECKPOINT_START = 0x46;
  public static final int TAP_CHECKPOINT_END = 0x47;

  public static byte getKind(final byte opcode) {
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
}

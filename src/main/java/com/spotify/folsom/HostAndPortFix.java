/*
 * Copyright (c) 2017 Spotify AB
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

import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Workaround for compatibility issues when upgrading Guava from 21 to 22
 */
public class HostAndPortFix {

  private static final HostAndPort LOCALHOST = HostAndPort.fromHost("localhost");

  private static final Method GET_HOST_TEXT = find(LOCALHOST, "getHostText");
  private static final Method GET_HOST = find(LOCALHOST, "getHost");

  private static final Method METHOD = GET_HOST != null ? GET_HOST : GET_HOST_TEXT;

  private static Method find(final HostAndPort testObj, final String methodName) {
    try {
      final Method method = testObj.getClass().getMethod(methodName);
      final String value = (String) method.invoke(testObj);
      if ("localhost".equals(value)) {
        return method;
      } else {
        return null;
      }
    } catch (NoSuchMethodException e) {
      return null;
    } catch (InvocationTargetException e) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    }
  }

  public static String getHostText(HostAndPort hostAndPort) {
    try {
      return (String) METHOD.invoke(hostAndPort);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

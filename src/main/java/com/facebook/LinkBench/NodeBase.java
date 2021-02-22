/*
 * Copyright 2012, Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.LinkBench;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

/**
 * Object node in social graph
 * @author tarmstrong
 */
public abstract class NodeBase {
  public static final int USER_TYPE = 1;
  public static final int DEVICE_TYPE = 2;
  public static final int IP_TYPE = 3;
  public abstract String[] fields();
  public abstract int getType();
  public abstract boolean fieldIsString(int idx);
  public abstract Object getField(int idx);
  public String getFieldString(int idx) {
    if (fieldIsString(idx)) {
      return "\"" + stringLiteral((byte[])getField(idx)) + "\"";
    } else {
      return getField(idx).toString();
    }
  }

  public NodeBase() {}

  public abstract String toString();

  public static String TypeIDToName(int type) {
    switch (type) {
      case USER_TYPE:
        return "User";
      case DEVICE_TYPE:
        return "Device";
      case IP_TYPE:
        return "IP";
      case 2048:
        return "Node2048";
      default:
        System.exit(1);
        return "";
      }
  }
  private static String stringLiteral(byte arr[]) {
    CharBuffer cb = Charset.forName("ISO-8859-1").decode(ByteBuffer.wrap(arr));
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < cb.length(); i++) {
      char c = cb.get(i);
      switch (c) {
        case '\"':
          sb.append("\\\"");
          break;
        case '\'':
          sb.append("\\'");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\0':
          sb.append("\\0");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '\f':
          sb.append("\\f");
          break;
        default:
          // if (Character.getNumericValue(c) < 0) {
          //   // Fall back on hex string for values not defined in latin-1
          //   return hexStringLiteral(arr);
          // } else {
            sb.append(c);
          // }
      }
    }
    return sb.toString();
  }
}

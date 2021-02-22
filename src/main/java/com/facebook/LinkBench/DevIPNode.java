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

/**
 * Object node in social graph
 * @author tarmstrong
 */
public abstract class DevIPNode extends NodeBase {
  protected static String[] _fields = {"address", "time"};
  public String[] fields() {
    return _fields;
  }
  public boolean fieldIsString(int idx) {
    return idx == 0;
  }
  public Object getField(int idx) {
    switch (idx) {
      case 0: return address;
      case 1: return time;
      default: assert(false); return null;
    }
  }
  protected abstract String embedAddr();
  @Override
  public String getFieldString(int idx) {
    // TODO Auto-generated method stub
    switch (idx) {
      case 0: return embedAddr();
      case 1: return Long.toString(time);
      default: assert(false); return null;
    }
  }

  /** ip or mac address */
  public byte address[];
  /** Last update time of node as UNIX timestamp */
  public long time;


  public DevIPNode(byte address[], long time) {
    super();
    this.address = address;
    this.time = time;
  }
}

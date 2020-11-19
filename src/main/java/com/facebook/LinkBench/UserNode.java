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
public class UserNode extends NodeBase {
  private static String[] _fields = {"id", "liveness", "time"};
  public String[] fields() {
    return _fields;
  }
  public boolean fieldIsString(int idx) {
    return false;
  }
  public int getType() { return 1; }
  public Object getField(int idx) {
    switch (idx) {
      case 0: return id;
      case 1: return liveness;
      case 2: return time;
      default: assert(false); return null;
    }
  }
  /** Unique identifier for node */
  public long id;

  /** Type of node */

  /** Version of node: typically updated on every change */
  public long liveness;

  /** Last update time of node as UNIX timestamp */
  public long time;

  public UserNode(long id, long liveness, long time) {
    super();
    this.id = id;
    this.liveness = liveness;
    this.time = time;
  }

  public UserNode clone() {
    return new UserNode(id, liveness, time);
  }
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof UserNode)) {
      return false;
    }
    UserNode o = (UserNode) other;
    return id == o.id && liveness == o.liveness
        && time == o.time;
  }

  public String toString() {
    return "UserNode(" + "id=" + id + ",liveness=" + liveness + ","
                   + "timestamp=" + time + ")";
  }
}

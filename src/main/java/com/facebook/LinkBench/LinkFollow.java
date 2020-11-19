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

/** TODO: should we just remove id2? */
public class LinkFollow extends LinkBase {
  public int getType() { return FOLLOW_TYPE; }
  private static String[] _fields = {"id1", "id2", "time"};
  public String[] fields() {
    return _fields;
  }
  public boolean fieldIsString(int idx) {
    return false;
  }
  public Object getField(int idx) {
    switch (idx) {
      case 0: return id1;
      case 1: return id2;
      case 2: return time;
      default: assert(false); return null;
    }
  }

  public LinkFollow(long id1, long id2, long time) {
    super();
    this.id1 = id1;
    this.id2 = id2;
    this.time = time;
  }

  LinkFollow() {
    super();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof LinkFollow) {
      LinkFollow o = (LinkFollow) other;
      return id1 == o.id1 && id2 == o.id2 &&
          time == o.time;
    } else {
      return false;
    }
  }

  public String toString() {
    return String.format("LinkFollow(id1=%d, id2=%d, " +
        "time=%d", id1, id2, time);
  }

  /**
   * Clone an existing link
   * @param l
   */
  public LinkFollow clone() {
    LinkFollow l = new LinkFollow();
    l.id1 = this.id1;
    l.id2 = this.id2;
    l.time = this.time;
    return l;
  }
  /** The node id of the source of directed edge */
  public long id1;

  /** The node id of the target of directed edge */
  public long id2;

  public long time;
}

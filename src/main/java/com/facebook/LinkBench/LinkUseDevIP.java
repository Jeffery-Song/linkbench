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
public class LinkUseDevIP extends LinkBase {
  public int getType() { return type; }
  private static String[] _fields = {"id1", "address", "time"};
  public String[] fields() {
    return _fields;
  }
  public boolean fieldIsString(int idx) {
    return idx == 1;
  }
  public Object getField(int idx) {
    switch (idx) {
      case 0: return id1;
      case 1: return address;
      case 2: return time;
      default: assert(false); return null;
    }
  }

  public LinkUseDevIP(long id1, long id2, byte[] address, long time, int type) {
    super();
    this.id1 = id1;
    this.id2 = id2;
    this.address = address;
    this.time = time;
    this.type = type;
  }

  LinkUseDevIP() {
    super();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof LinkUseDevIP) {
      LinkUseDevIP o = (LinkUseDevIP) other;
      return id1 == o.id1 && id2 == o.id2 &&
          time == o.time && type == o.type;
    } else {
      return false;
    }
  }

  public String toString() {
    return String.format("LinkUseDevIP(id1=%d, address=%s, " +
        "time=%d", id1, address.toString(), time);
  }

  /**
   * Clone an existing link
   * @param l
   */
  public LinkUseDevIP clone() {
    LinkUseDevIP l = new LinkUseDevIP();
    l.id1 = this.id1;
    l.id2 = this.id2;
    l.address = this.address.clone();
    l.time = this.time;
    l.type = this.type;
    return l;
  }
  /** The node id of the source of directed edge */
  public long id1;

  /** The node id of the target of directed edge */
  /** this field is only for internal use. outside database can only observe address string */
  public long id2;
  public byte[] address;

  public long time;

  // for use dev, 11; for use ip, 12;
  public int type;
}

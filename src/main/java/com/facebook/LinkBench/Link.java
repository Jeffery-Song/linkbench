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

import java.util.Arrays;


public class Link extends LinkBase {
  private static String[] _fields = {"id1", "id2", "link_type", "visibility", "data", "version", "time"};
  public String[] fields() {
    return _fields;
  }
  public int getType() { return (int)link_type; }
  public boolean fieldIsString(int idx) {
    return idx == 4;
  }
  public Object getField(int idx) {
    switch (idx) {
      case 0: return id1;
      case 1: return id2;
      case 2: return link_type;
      case 3: return visibility;
      case 4: return data;
      case 5: return version;
      case 6: return time;
      default: assert(false); return null;
    }
  }

  public Link(long id1, long link_type, long id2,
      byte visibility, byte[] data, int version, long time) {
    super();
    this.id1 = id1;
    this.link_type = link_type;
    this.id2 = id2;
    this.visibility = visibility;
    this.data = data;
    this.version = version;
    this.time = time;
  }

  Link() {
    super();
    link_type = LinkBase.LINKBENCH_DEFAULT_TYPE;
    visibility = LinkStore.VISIBILITY_DEFAULT;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Link) {
      Link o = (Link) other;
      return id1 == o.id1 && id2 == o.id2 &&
          link_type == o.link_type &&
          visibility == o.visibility &&
          version == o.version && time == o.time &&
          Arrays.equals(data, o.data);
    } else {
      return false;
    }
  }

  public String toString() {
    return String.format("Link(id1=%d, id2=%d, link_type=%d," +
        "visibility=%d, version=%d," +
        "time=%d, data=%s", id1, id2, link_type,
        visibility, version, time, data.toString());
  }

  /**
   * Clone an existing link
   * @param l
   */
  public Link clone() {
    Link l = new Link();
    l.id1 = this.id1;
    l.link_type = this.link_type;
    l.id2 = this.id2;
    l.visibility = this.visibility;
    l.data = this.data.clone();
    l.version = this.version;
    l.time = this.time;
    return l;
  }

  /** The node id of the source of directed edge */
  public long id1;

  /** The node id of the target of directed edge */
  public long id2;

  /** Type of link */
  public long link_type;

  /** Visibility mode */
  public byte visibility;

  /** Version of link */
  public int version;

  /** time is the sort key for links.  Often it contains a timestamp,
      but it can be used as a arbitrary user-defined sort key. */
  public long time;

  /** Arbitrary payload data */
  public byte[] data;

}

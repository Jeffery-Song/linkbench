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
public class LinkReferrer extends LinkBase {
  public int getType() { return REFERRER_TYPE; }
  private static String[] _fields = {"id1", "id2"};
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
      default: assert(false); return null;
    }
  }

  public LinkReferrer(long id1, long id2) {
    super();
    this.id1 = id1;
    this.id2 = id2;
  }

  LinkReferrer() {
    super();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof LinkReferrer) {
      LinkReferrer o = (LinkReferrer) other;
      return id1 == o.id1 && id2 == o.id2;
    } else {
      return false;
    }
  }

  public String toString() {
    return String.format("LinkReferrer(id1=%d, id2=%d, ",
        id1, id2);
  }

  /**
   * Clone an existing link
   * @param l
   */
  public LinkReferrer clone() {
    LinkReferrer l = new LinkReferrer();
    l.id1 = this.id1;
    l.id2 = this.id2;
    return l;
  }
  /** The node id of the source of directed edge */
  public long id1;

  /** The node id of the target of directed edge */
  public long id2;
}

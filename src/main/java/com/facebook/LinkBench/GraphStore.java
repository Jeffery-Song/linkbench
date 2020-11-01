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

import java.util.List;

/**
 * An abstract class for storing both nodes and edges
 * @author tarmstrong
 */
public abstract class GraphStore extends LinkStore implements NodeStore {

  /** Provide generic implementation */
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    long ids[] = new long[nodes.size()];
    int i = 0;
    for (Node node: nodes) {
      long id = addNode(dbid, node);
      ids[i++] = id;
    }
    return ids;
  }
  public int getRetries() {
    return 0;
  }


  // ali link store
  // return all links
  public Node[] aliGetFan(long id) throws Exception {
    throw new Exception("Not implemented aliGetFan");
  }
  // return all nodes
  public Node[] aliGetFollow(long id) throws Exception {
    throw new Exception("Not implemented aliGetFollow");
  }
  public Node[] aliRecom(long id) throws Exception {
    throw new Exception("Not implemented aliRecom");
  }
  public boolean aliFollow(Link l) throws Exception {
    throw new Exception("Not implemented aliFollow");
  }
  public boolean aliUnfollow(long id1, long id2) throws Exception {
    throw new Exception("Not implemented aliUnfollow");
  }


  // ali node store
  // returns updated node count
  public long aliLogin(long id) throws Exception {
    throw new Exception("Not implemented aliLogin");
  }
  // returns updated node count, new node id is returned through node.id
  public long aliReg(Node node) throws Exception {
    throw new Exception("Not implemented aliReg");
  }
  // returns updated node count, new node id is returned through node.id
  public long aliRegRef(Node node, long referrer) throws Exception {
    throw new Exception("Not implemented aliRegRef");
  }
  // nothing to return
  public boolean aliPay(long id1, long id2) throws Exception {
    throw new Exception("Not implemented aliPay");
  }
}

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

import java.io.IOException;
import java.util.Properties;
import com.facebook.LinkBench.MemoryLinkStore;

/**
 * Simple in-memory implementation of GraphStore
 * Not efficient or optimized at all, just for testing purposes.
 *
 * MemoryLinkStoreClient instances sharing the same data can be created
 * using the newInstance() method.
 * MemoryLinkStoreClient can be accessed concurrently from multiple threads,
 * but a simple mutex is used so there is no internal concurrency (requests
 * are serialized)
 */
public class MemoryLinkStoreClient extends GraphStore {
  private static MemoryLinkStore mls = new MemoryLinkStore();

  /**
   * Create a new MemoryLinkStoreClient instance with fresh data
   */
  public MemoryLinkStoreClient() {
    System.err.println("MLS client loaded, singleton is " + mls);
  }


  @Override
  public void initialize(Properties p, Phase currentPhase, int threadId)
      throws IOException, Exception {
  }

  @Override
  public void close() {
  }

  @Override
  public void clearErrors(int threadID) {
  }

  @Override
  public boolean addLink(String dbid, Link a, boolean noinverse) 
      throws Exception {
    return mls.addLink(dbid, a, noinverse);
  }

  @Override
  public boolean deleteLink(String dbid, long id1, long link_type, long id2,
      boolean noinverse, boolean expunge) throws Exception {
    return mls.deleteLink(dbid, id1, link_type, id2, noinverse, expunge);
  }

  @Override
  public boolean updateLink(String dbid, Link a, boolean noinverse)
      throws Exception {
    return mls.updateLink(dbid, a, noinverse);
  }

  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
      throws Exception {
    return mls.getLink(dbid, id1, link_type, id2);
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type)
      throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
      long minTimestamp, long maxTimestamp, int offset, int limit)
      throws Exception {
    return mls.getLinkList(dbid, id1, link_type, minTimestamp, 
                           maxTimestamp, offset, limit);
  }

  @Override
  public long countLinks(String dbid, long id1, long link_type)
      throws Exception {
    return mls.countLinks(dbid, id1, link_type);
  }


  @Override
  public void resetNodeStore(String dbid, long startID) {
    mls.resetNodeStore(dbid, startID);
  }

  @Override
  public long addNode(String dbid, Node node) throws Exception {
    return mls.addNode(dbid, node);
  }

  @Override
  public Node getNode(String dbid, int type, long id) throws Exception {
    return mls.getNode(dbid, type, id);
  }

  @Override
  public boolean updateNode(String dbid, Node node) throws Exception {
    return mls.updateNode(dbid, node);
  }

  @Override
  public boolean deleteNode(String dbid, int type, long id) throws Exception {
    return mls.deleteNode(dbid, type, id);
  }
}

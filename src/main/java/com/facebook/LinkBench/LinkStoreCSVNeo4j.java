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

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.ResultSet;
// import java.sql.Neo4jException;
// import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Map;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * we assume that GraphStore is not concurrently accessed, so this is not thread safe.
 */
public class LinkStoreCSVNeo4j extends GraphStore {
  private class MyTuple {
    public int ret;
    public long time;
  }

  public static final String CONFIG_MAX_FILE_SIZE = "csv_file_max_size";

  public static final int DEFAULT_MAX_FILE_SIZE = 100000;

  private static final boolean INTERNAL_TESTING = false;

  private static AtomicLong _nodeid;
  /* loader id, use this to seperate different node/link files. */
  private static AtomicLong _loaderid = new AtomicLong(0);

  private long loaderid = 0;
  private long file_idx = 0; // seperate to multiple file
  private HashMap<Integer, ArrayList<Node>> type_to_nodes = new HashMap<Integer, ArrayList<Node>>();
  private HashMap<Long, ArrayList<Link>> type_to_links = new HashMap<Long, ArrayList<Link>>();

  Level debuglevel;

  private Phase phase;

  int max_file_size = DEFAULT_MAX_FILE_SIZE;
  // Optional optimization: disable binary logging
  // boolean disableBinLogForLoad = false;

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  public LinkStoreCSVNeo4j() {
    super();
    loaderid = _loaderid.getAndIncrement();
  }

  public LinkStoreCSVNeo4j(Properties props) throws IOException, Exception {
    super();
    loaderid = _loaderid.getAndIncrement();
    initialize(props, Phase.LOAD, 0);
  }

  static private String loadFromFile(String filePath) {
    try {
      return new String(Files.readAllBytes(Paths.get(filePath)));
    } catch (IOException e) {
      assert(false);
      return "";
    }
  }

  public void initialize(Properties props, Phase currentPhase,
    int threadId) throws IOException, Exception {
    if (currentPhase == Phase.LOAD) {
      _nodeid = new AtomicLong(1);
    } else {
      throw new Exception("Only use CSVNeo4j in load phase.");
    }

    debuglevel = ConfigUtil.getDebugLevel(props);
    phase = currentPhase;

    if (props.containsKey(CONFIG_MAX_FILE_SIZE)) {
      max_file_size = ConfigUtil.getInt(props, CONFIG_MAX_FILE_SIZE);
    }
    logger.error("bulk size is " + max_file_size);
    // if (props.containsKey(CONFIG_DISABLE_BINLOG_LOAD)) {
    //   disableBinLogForLoad = ConfigUtil.getBool(props,
    //                                   CONFIG_DISABLE_BINLOG_LOAD);
    // }
  }

  @Override
  public void close() {
    System.err.println("closing csv store");
    for(Map.Entry<Integer, ArrayList<Node>> entry : type_to_nodes.entrySet()) {
      Integer type = entry.getKey();
      ArrayList<Node> list = entry.getValue();
      if (list.size() == 0) continue;
      flush_node_file(list, type);
    }
    for(Map.Entry<Long, ArrayList<Link>> entry : type_to_links.entrySet()) {
      Long type = entry.getKey();
      ArrayList<Link> list = entry.getValue();
      if (list.size() == 0) continue;
      flush_link_file(list, type);
    }
  }

  public void clearErrors(int threadID) { }

  @Override
  public boolean addLink(String dbid, final Link l, boolean noinverse)
      throws Exception {
    System.err.println("adding a link");
    ArrayList<Link> list = type_to_links.get(l.link_type);
    if (list == null) {
      list = new ArrayList<Link>();
      type_to_links.put(l.link_type, list);
    }
    list.add(l);
    check_flush_link_file(list, l.link_type);
    return true;
  }

  private String formatLink(Link l) {
    StringBuilder sb = new StringBuilder();
    sb.append("" + l.id1 + "|" + l.id2);
    for (int i = 0; i < l.fields().length; i++) {
      if (l.fields()[i].equals("id1")) continue;
      if (l.fields()[i].equals("id2")) continue;
      // if (l.fields()[i].equals("link_type")) continue;
      if (l.fieldIsString(i)) 
        sb.append("|\"" + stringLiteral((byte[])l.getField(i)) + "\"");
      else
        sb.append("|" + l.getField(i).toString());
    }
    return sb.append("\n").toString();
  }
  private void flush_link_file(ArrayList<Link> list, long key) {
    String fname = "Link_" + loaderid + "_" + file_idx + ".csv";
    file_idx += 1;
    try {
      FileWriter fos = new FileWriter(fname);
      for (Link l : list) {
        fos.append(formatLink(l));
      }
      fos.close();
      list.clear();
    } catch (IOException e) {
      assert(false);
    }
  }
  private void check_flush_link_file(ArrayList<Link> list, long key) {

    if (list.size() < max_file_size) {
      return;
    }
    flush_link_file(list, key);
  }

  
  @Override
  public void addBulkLinks(String dbid, final List<Link> links, boolean noinverse)
  throws Exception {
    System.err.println("adding a bulk of links, size=" + links.size());
    for (Link l : links) {
      ArrayList<Link> list = type_to_links.get(l.link_type);
      if (list == null) {
        list = new ArrayList<Link>();
        type_to_links.put(l.link_type, list);
      }
      list.add(l);
      check_flush_link_file(list, l.link_type);
    }
  }

  @Override
  public boolean deleteLink(String dbid, final long id1, final long link_type, final long id2,
                         boolean noinverse, boolean expunge)
      throws Exception {
    throw new Exception("not allowed in csv");
  }

  @Override
  public boolean updateLink(String dbid, Link l, boolean noinverse)
      throws Exception {
    throw new Exception("not allowed in csv");
  }


  // lookup using id1, type, id2
  @Override
  public Link getLink(String dbid, final long id1, final long link_type, final long id2)
      throws Exception {
    throw new Exception("not allowed in csv");
  }
  @Override
  public Link[] multigetLinks(String dbid, final long id1, final long link_type,
                            final long[] id2s)
      throws Exception {
    throw new Exception("not allowed in csv");
  }

  // lookup using just id1, type
  @Override
  public Link[] getLinkList(String dbid, final long id1, final long link_type)
      throws Exception {
    throw new Exception("not allowed in csv");
  }

  @Override
  public Link[] getLinkList(String dbid, final long id1, final long link_type,
                            final long minTimestamp, final long maxTimestamp,
                            final int offset, final int limit)
      throws Exception {
    throw new Exception("not allowed in csv");
  }

  // count the #links
  @Override
  public long countLinks(String dbid, final long id1, long link_type)
      throws Exception {
    throw new Exception("not allowed in csv");
  }

  @Override
  public int bulkLoadBatchSize() {
    return max_file_size;
  }

  @Override
  public void addBulkCounts(String dbid, List<LinkCount> a)
      throws Exception {
    /** bulk load always use this, just ignore it */
    // throw new Exception("not allowed in csv");
  }

  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception { }

  @Override
  public long addNode(String dbid, final Node node) throws Exception {
    final long allocatedID = _nodeid.getAndIncrement();
    node.id = allocatedID;
    ArrayList<Node> list = type_to_nodes.get(node.type);
    if (list == null) {
      list = new ArrayList<Node>();
      type_to_nodes.put(node.type, list);
    }
    list.add(node);
    check_flush_node_file(list, node.type);
    return allocatedID;
  }

  private String formatNode(Node n) {
    StringBuilder sb = new StringBuilder();
    sb.append("" + n.id);
    for (int i = 0; i < n.fields().length; i++) {
      if (n.type != 2048) System.err.println("We hanve more than one node type");
      if (n.fields()[i].equals("id")) continue;
      // if (n.fields()[i].equals("type")) continue;
      if (n.fieldIsString(i)) 
        sb.append("|\"" + stringLiteral((byte[])n.getField(i)) + "\"");
      else
        sb.append("|" + n.getField(i).toString());
    }
    return sb.append("\n").toString();
  }

  private void check_flush_node_file(ArrayList<Node> list, int key) {
    if (list.size() < max_file_size) {
      return;
    }
    flush_node_file(list, key);
  }
  private void flush_node_file(ArrayList<Node> list, int key) {
    String fname = "Node_" + loaderid + "_" + file_idx + ".csv";
    file_idx += 1 ;
    try {
      FileWriter fos = new FileWriter(fname);
      for (Node n : list) {
        fos.append(formatNode(n));
      }
      fos.close();
      list.clear();
    } catch (Exception e) {
      assert(false);
    }
  }

  // @Override
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    long ids[] = new long[nodes.size()];
    for (int i = 0; i < nodes.size(); i++) {
      Node node = nodes.get(i);
      final long allocatedID = _nodeid.getAndIncrement();
      ids[i] = allocatedID;
      node.id = allocatedID;
      ArrayList<Node> list = type_to_nodes.get(node.type);
      if (list == null) {
        list = new ArrayList<Node>();
        type_to_nodes.put(node.type, list);
      }
      list.add(node);
      check_flush_node_file(list, node.type);
    }
    return ids;
  }

  @Override
  public Node getNode(String dbid, final int type, final long id) throws Exception {
    throw new Exception("not allowed in csv");
  }

  @Override
  public boolean updateNode(String dbid, final Node node) throws Exception {
    throw new Exception("not allowed in csv");
  }

  @Override
  public boolean deleteNode(String dbid, final int type, final long id) throws Exception {
    throw new Exception("not allowed in csv");
  }

  /**
   * Convert a byte array into a valid mysql string literal, assuming that
   * it will be inserted into a column with latin-1 encoding.
   * Based on information at
   * http://dev.mysql.com/doc/refman/5.1/en/string-literals.html
   * @param arr
   * @return
   */
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

  /**
   * Create a mysql hex string literal from array:
   * E.g. [0xf, bc, 4c, 4] converts to x'0fbc4c03'
   * @param arr
   * @return the mysql hex literal including quotes
   */
  private static String hexStringLiteral(byte[] arr) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < arr.length; i++) {
      byte b = arr[i];
      int lo = b & 0xf;
      int hi = (b >> 4) & 0xf;
      sb.append(Character.forDigit(hi, 16));
      sb.append(Character.forDigit(lo, 16));
    }
    return sb.toString();
  }
  public static void main(String[] args) { }

}

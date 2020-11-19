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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * we assume that GraphStore is not concurrently accessed, so this is not thread safe.
 */
public class GraphSerializerCSV {
  public static final String CONFIG_MAX_FILE_SIZE = "csv_file_max_size";

  public static final int DEFAULT_MAX_FILE_SIZE = 100000;

  private static AtomicLong _loaderid = new AtomicLong(0);

  private long loaderid = 0;
  private long file_idx = 0; // seperate to multiple file
  private HashMap<Integer, ArrayList<NodeBase>> type_to_nodes = new HashMap<Integer, ArrayList<NodeBase>>();
  private HashMap<Integer, ArrayList<LinkBase>> type_to_links = new HashMap<Integer, ArrayList<LinkBase>>();

  Level debuglevel;

  int max_file_size = DEFAULT_MAX_FILE_SIZE;

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  public GraphSerializerCSV() {
    super();
    loaderid = _loaderid.getAndIncrement();
  }

  public GraphSerializerCSV(Properties props) throws IOException, Exception {
    super();
    loaderid = _loaderid.getAndIncrement();
    initialize(props, Phase.LOAD, 0);
  }

  public void initialize(Properties props, Phase currentPhase,
    int threadId) throws IOException, Exception {
    if (currentPhase != Phase.LOAD) {
      throw new Exception("Only use CSVNeo4j in load phase.");
    }

    debuglevel = ConfigUtil.getDebugLevel(props);

    if (props.containsKey(CONFIG_MAX_FILE_SIZE)) {
      max_file_size = ConfigUtil.getInt(props, CONFIG_MAX_FILE_SIZE);
    }
    logger.error("bulk size is " + max_file_size);
  }

  public void close() {
    System.err.println("closing csv store");
    for(Map.Entry<Integer, ArrayList<NodeBase>> entry : type_to_nodes.entrySet()) {
      Integer type = entry.getKey();
      ArrayList<NodeBase> list = entry.getValue();
      if (list.size() == 0) continue;
      flush_node_file(list, type);
    }
    for(Map.Entry<Integer, ArrayList<LinkBase>> entry : type_to_links.entrySet()) {
      Integer type = entry.getKey();
      ArrayList<LinkBase> list = entry.getValue();
      if (list.size() == 0) continue;
      flush_link_file(list, type);
    }
  }

  public boolean addLink(String dbid, final LinkBase l, boolean noinverse)
      throws Exception {
    System.err.println("adding a link");
    ArrayList<LinkBase> list = type_to_links.get((int)l.getType());
    if (list == null) {
      list = new ArrayList<LinkBase>();
      type_to_links.put(l.getType(), list);
    }
    list.add(l);
    check_flush_link_file(list, l.getType());
    return true;
  }

  private String formatLink(LinkBase l) {
    StringBuilder sb = new StringBuilder();
    // sb.append("" + l.id1 + "|" + l.id2);
    for (int i = 0; i < l.fields().length; i++) {
      // if (l.fields()[i].equals("id1")) continue;
      // if (l.fields()[i].equals("id2")) continue;
      // if (l.fields()[i].equals("link_type")) continue;
      if (i != 0) {
        sb.append("|");
      }
      if (l.fieldIsString(i)) {
        sb.append("\"" + stringLiteral((byte[])l.getField(i)) + "\"");
      } else {
        sb.append("" + l.getField(i).toString());
      }
    }
    return sb.append("\n").toString();
  }
  private void flush_link_file(ArrayList<LinkBase> list, int key) {
    String fname = LinkBase.TypeIDToName(key) + "_" + loaderid + "_" + file_idx + ".csv";
    file_idx += 1;
    try {
      FileWriter fos = new FileWriter(fname);
      for (LinkBase l : list) {
        fos.append(formatLink(l));
      }
      fos.close();
      list.clear();
    } catch (IOException e) {
      assert(false);
    }
  }
  private void check_flush_link_file(ArrayList<LinkBase> list, int key) {

    if (list.size() < max_file_size) {
      return;
    }
    flush_link_file(list, key);
  }

  public void addBulkLinks(String dbid, final List<LinkBase> links, boolean noinverse)
  throws Exception {
    System.err.println("adding a bulk of links, size=" + links.size());
    for (LinkBase l : links) {
      ArrayList<LinkBase> list = type_to_links.get(l.getType());
      if (list == null) {
        list = new ArrayList<LinkBase>();
        type_to_links.put(l.getType(), list);
      }
      list.add(l);
      check_flush_link_file(list, l.getType());
    }
  }

  public int bulkLoadBatchSize() {
    return max_file_size;
  }

  public void addNode(String dbid, final NodeBase node) throws Exception {
    ArrayList<NodeBase> list = type_to_nodes.get(node.getType());
    if (list == null) {
      list = new ArrayList<NodeBase>();
      type_to_nodes.put(node.getType(), list);
    }
    list.add(node);
    check_flush_node_file(list, node.getType());
  }

  private String formatNode(NodeBase n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n.fields().length; i++) {
      if (i != 0) {
        sb.append("|");
      }
      if (n.fieldIsString(i)) {
        sb.append("\"" + stringLiteral((byte[])n.getField(i)) + "\"");
      } else {
        sb.append("" + n.getField(i).toString());
      }
    }
    return sb.append("\n").toString();
  }

  private void check_flush_node_file(ArrayList<NodeBase> list, int key) {
    if (list.size() < max_file_size) {
      return;
    }
    flush_node_file(list, key);
  }
  private void flush_node_file(ArrayList<NodeBase> list, int key) {
    String fname = NodeBase.TypeIDToName(key) + "_" + loaderid + "_" + file_idx + ".csv";
    file_idx += 1 ;
    try {
      FileWriter fos = new FileWriter(fname);
      for (NodeBase n : list) {
        fos.append(formatNode(n));
      }
      fos.close();
      list.clear();
    } catch (Exception e) {
      assert(false);
    }
  }

  // @Override
  public void bulkAddNodes(String dbid, List<NodeBase> nodes) throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      NodeBase node = nodes.get(i);
      ArrayList<NodeBase> list = type_to_nodes.get(node.getType());
      if (list == null) {
        list = new ArrayList<NodeBase>();
        type_to_nodes.put(node.getType(), list);
      }
      list.add(node);
      check_flush_node_file(list, node.getType());
    }
    // return ids;
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
}

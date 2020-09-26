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

import org.neo4j.driver.v1.exceptions.*;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.facebook.LinkBench.measurements.Measurements;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LinkStoreNeo4j extends GraphStore {
  private class MyTuple {
    public int ret;
    public long time;
  }

  /* MySql database server configuration keys */
  public static final String CONFIG_HOST = "host";
  public static final String CONFIG_PORT = "port";
  public static final String CONFIG_USER = "user";
  public static final String CONFIG_PASSWORD = "password";
  public static final String CONFIG_BULK_INSERT_BATCH = "mysql_bulk_insert_batch";
  public static final String CONFIG_DISABLE_BINLOG_LOAD = "mysql_disable_binlog_load";

  // cypher query files
  public static final String CONFIG_ADD_LINK_QUERY = "neo4j.add_link.cypher";
  public static final String CONFIG_ADD_NODE_QUERY = "neo4j.add_node.cypher";
  public static final String CONFIG_GET_LINK_LIST_QUERY = "neo4j.get_link_list.cypher";
  public static final String CONFIG_GET_NODE_QUERY = "neo4j.get_node.cypher";
  // public static final String CONFIG_UPDATE_LINK_QUERY = "neo4j.update_link.cypher";
  public static final String CONFIG_DELETE_LINK_QUERY = "neo4j.delete_link.cypher";
  public static final String CONFIG_DELETE_LINK_EXPUNGE_QUERY = "neo4j.delete_link_expunge.cypher";
  public static final String CONFIG_DELETE_NODE_QUERY = "neo4j.delete_node.cypher";
  public static final String CONFIG_MULTIGET_LINK_QUERY = "neo4j.multiget_link.cypher";
  public static final String CONFIG_COUNT_LINK_QUERY = "neo4j.count_link.cypher";
  public static final String CONFIG_UPDATE_NODE_QUERY = "neo4j.update_node.cypher";
  public static final String CONFIG_UPDATE_NODE_GIVEN_PATH_QUERY = "neo4j.update_node_given_path.cypher";

  public static final int DEFAULT_BULKINSERT_SIZE = 20;

  private static final boolean INTERNAL_TESTING = false;

  private static boolean _dbidready = false;
  private static String _dbid = "";
  private static String __sync = "";
  private static AtomicLong _nodeid;

  private Measurements _measurements = Measurements.getMeasurements();

  private boolean use_given_path = false;

  private String add_link_query;
  private String add_node_query;
  private String get_link_list_query;
  private String get_node_query;
  // private String update_link_query;
  private String update_node_query;
  private String delete_link_query;
  private String delete_link_expunge_query;
  private String delete_node_query;
  private String multiget_link_query;
  private String count_link_query;
  // private String template_query;

  // String linktable;
  // String counttable;
  // String nodetable;

  String host;
  String user;
  String pwd;
  String port;

  long addlinkcount = 0;
  long addlinktime = 0;
  long addlinktxruntime = 0;
  // String defaultDB;

  Level debuglevel;
  // Use read-only and read-write connections and statements to avoid toggling
  // auto-commit.
  // Connection conn_ro, conn_rw;
  // Statement stmt_ro, stmt_rw;
  private Driver driver = null;
  private static Driver driver_s = null;
  // private Session session;

  private Phase phase;

  int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;
  // Optional optimization: disable binary logging
  // boolean disableBinLogForLoad = false;

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  public int retries = 0;
  @Override
  public int getRetries() {
    return retries;
  }

  public LinkStoreNeo4j() {
    super();
  }

  public LinkStoreNeo4j(Properties props) throws IOException, Exception {
    super();
    // template_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_template_QUERY));
    initialize(props, Phase.LOAD, 0);
  }
  private void checkDBID(String dbid) {
    if (_dbidready == false) {
      synchronized(__sync) {
        if (_dbidready == false) {
          _dbid = dbid;
          _dbidready = true;
        }
      }
    }
    if (_dbid != dbid) {
      System.exit(1);
    }
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
      // long maxId = ConfigUtil.getLong(props, Config.MAX_ID);
      _nodeid = new AtomicLong(ConfigUtil.getLong(props, com.facebook.LinkBench.Config.MAX_ID));
    }

    add_link_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_ADD_LINK_QUERY));
    add_node_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_ADD_NODE_QUERY));
    get_link_list_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_GET_LINK_LIST_QUERY));
    get_node_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_GET_NODE_QUERY));
    // update link is implemented by get link
    // update_link_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_UPDATE_LINK_QUERY));
    delete_link_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_DELETE_LINK_QUERY));
    delete_link_expunge_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_DELETE_LINK_EXPUNGE_QUERY));
    delete_node_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_DELETE_NODE_QUERY));
    multiget_link_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_MULTIGET_LINK_QUERY));
    count_link_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_COUNT_LINK_QUERY));
    use_given_path = ConfigUtil.getBool(props, "ali.request_via_explicit_path", false);
    if (use_given_path) {
      String fname = ConfigUtil.getPropertyRequired(props, CONFIG_UPDATE_NODE_GIVEN_PATH_QUERY);
      update_node_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_UPDATE_NODE_GIVEN_PATH_QUERY));
      // System.err.println("loading from " + fname);
    } else {
      String fname = ConfigUtil.getPropertyRequired(props, CONFIG_UPDATE_NODE_QUERY);
      update_node_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_UPDATE_NODE_QUERY));
      // System.err.println("loading from " + fname);
    }

    // update link is implemented by get link
    // update_link_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_UPDATE_LINK_QUERY));
    // template_query = loadFromFile(ConfigUtil.getPropertyRequired(props, CONFIG_template_QUERY));
  
    host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
    user = ConfigUtil.getPropertyRequired(props, CONFIG_USER);
    pwd = ConfigUtil.getPropertyRequired(props, CONFIG_PASSWORD);
    port = props.getProperty(CONFIG_PORT);
    // defaultDB = ConfigUtil.getPropertyRequired(props, Config.DBID);

    if (port == null || port.equals("")) port = "3306"; //use default port
    debuglevel = ConfigUtil.getDebugLevel(props);
    phase = currentPhase;

    if (props.containsKey(CONFIG_BULK_INSERT_BATCH)) {
      bulkInsertSize = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_BATCH);
    }

    logger.error("bulk size is " + bulkInsertSize);
    // connect
    try {
      openConnection();
    } catch (Exception e) {
      logger.error("error connecting to database:", e);
      throw e;
    }

    // linktable = ConfigUtil.getPropertyRequired(props, Config.LINK_TABLE);
  }

  private static synchronized void staticOpenConnection(String uri_, String user_, String pwd_) throws Exception {
    if (driver_s == null) {
      driver_s = GraphDatabase.driver(uri_, AuthTokens.basic(user_, pwd_));
      // try (Session session = driver_s.session()) {
      //   session.writeTransaction(new TransactionWork<Void>() {
      //     @Override 
      //     public Void execute(Transaction tx) {
      //       // tx.run("CREATE INDEX ON :nt(id)");
      //       tx.run("CREATE CONSTRAINT ON (n:nt) ASSERT n.id IS UNIQUE");
      //       return null;
      //     }
      //   });
      // } catch (Exception e) {
      //   throw e;
      // }
    }
  }

  // connects to test database
  private void openConnection() throws Exception {
    if (driver_s != null) {
      driver = driver_s;
      return;
    }
    String uri = "bolt://" + host + ":" + port;
    Config config = Config.builder()
            .withMaxTransactionRetryTime( 0, MILLISECONDS )
            .build();

    // driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ), config );
    driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pwd), config);
    // try (Session session = driver.session()) {
    //   session.writeTransaction(new TransactionWork<Void>() {
    //     @Override 
    //     public Void execute(Transaction tx) {
    //       // tx.run("CREATE INDEX ON :nt(id)");
    //       tx.run("CREATE CONSTRAINT ON (n:nt) ASSERT n.id IS UNIQUE");
    //       return null;
    //     }
    //   });
    // } catch (Exception e) {
    //   throw e;
    // }
  }

  @Override
  public void close() {
    driver.close();
  }

  public void clearErrors(int threadID) { }

  /**
   * Handle SQL exception by logging error and selecting how to respond
   * @param ex Neo4jException thrown by MySQL JDBC driver
   * @return true if transaction should be retried
   */
  private boolean processNeo4jException(Neo4jException ex, String op) {
    return false;
  }

  @Override
  public boolean addLink(String dbid, final Link l, boolean noinverse)
    throws Exception {
    /**
     * merge (a {id:0})
     * on create set a.data='hello', a.test=true
     * on match set  a.data='world'
     * with a, exists(a.test) as test 
     * remove a.test 
     * return test
     */
    final String insert = add_link_query.replace("$link_type", String.valueOf(l.link_type));
    try (Session session = driver.session()) {
      // Long ts1 = System.nanoTime();
      while (true) { // retry for deadlock
        try {

          MyTuple ret = session.writeTransaction(new TransactionWork<MyTuple>() {
            @Override 
            public MyTuple execute(Transaction tx) {
              MyTuple ret = new MyTuple();
              // true for covered create
              // System.err.println("Query is " + insert);
              StatementResult sr = tx.run(insert, Values.parameters(
                  "id1", l.id1,
                  "type", l.link_type,
                  "id2", l.id2,
                  "visibility", (int)l.visibility,
                  "data", stringLiteral(l.data),
                  "time", l.time,
                  "version", l.version));
              // long ts1 = System.nanoTime();
              if (sr.hasNext() == false) {
                ret.ret = 2;
                // long ts2 = System.nanoTime();
                // ret.time = ts2-ts1;
              } else {
                boolean b = sr.next().get(0).asBoolean();
                // long ts2 = System.nanoTime();
                ret.ret = 0; // 0 for fresh creat, 1 for covered, 2 for err
                if (b) {
                  ret.ret = 1;
                }
                // ret.time = ts2-ts1;
              }
              return ret;
            }
          });
          // Long ts2 = System.nanoTime();
          // addlinkcount ++;
          // addlinktime += ts2-ts1;
          // addlinktxruntime += ret.time;
          // if (addlinkcount % 2000 == 0) {
          //   System.err.println("Added " + addlinkcount + "links" + 
          //                    ", current avg is(ns) " + addlinktime/addlinkcount +
          //                    ", current tx runtime is " + addlinktxruntime/addlinkcount);
          //   addlinkcount = 0;
          //   addlinktime = 0;
          //   addlinktxruntime = 0;
          // }
          if (ret.ret == 2) {
            throw new MissingException("Adding link, missing node1.id=" + l.id1 + " or 2.id=" + l.id2);
          }
          return ret.ret == 1;
          // break;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock

    } catch (NoSuchRecordException e) {
      throw e;
    } catch (Exception e) {
      throw e;
    }

  }

  @Override
  public void addBulkLinks(String dbid, final List<Link> links, boolean noinverse)
      throws Exception {

        StringBuilder sb = new StringBuilder();
        // final String insertBeforeType = 
        sb.append("match (a:nt {id:$id1}), (b:nt {id : $id2}) create (a)-[r:`$link_type`]->(b) \n");
        sb.append(" set r.id1=$id1"                + 
                     ", r.type=$type"              +
                     ", r.id2=$id2"                +
                     ", r.visibility=$visibility " +
                     ", r.data=$data "             +
                     ", r.time=$time"              +
                     ", r.version=$version"        + "\n");
        final String query = sb.toString();

        try (Session session = driver.session()) {
          // Long ts1 = System.nanoTime();
          // for (final Link l : links) {
        
          while (true) { // retry for deadlock
            try {

              session.writeTransaction(new TransactionWork<Void>() {
                @Override 
                public Void execute(Transaction tx) {
                  for (Link l : links) {
                    // System.out.println(query.replace("$link_type", String.valueOf(l.link_type)));
                    tx.run(query.replace("$link_type", String.valueOf(l.link_type)), Values.parameters(
                        "id1", l.id1,
                        "type", l.link_type,
                        "id2", l.id2,
                        "visibility", (int)l.visibility,
                        "data", stringLiteral(l.data),
                        "time", l.time,
                        "version", l.version));
                  }
                  return null;
                }
              });

              break;
            } catch (TransientException e) {
              retries += 1;
              continue; // retry for deadlock
            }
          } // end while, retry  for deadlock

          // }
        } catch (NoSuchRecordException e) {
          throw e;
        } catch (Exception e) {
          throw e;
        }
  }

  @Override
  public boolean deleteLink(String dbid, final long id1, final long link_type, final long id2,
                         boolean noinverse, boolean expunge)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    String query_template = delete_link_query;
    if (expunge) {
      query_template = delete_link_expunge_query;
    }
    final String query = query_template.replace("$link_type", String.valueOf(link_type));

    // final String query = sb.toString();
    try (Session session = driver.session()) {

      while (true) { // retry for deadlock
        try {
          return session.writeTransaction(new TransactionWork<Boolean>() {
            @Override 
            public Boolean execute(Transaction tx) {
              return tx.run(query, Values.parameters("id1", id1, "id2", id2, "vis", (int)VISIBILITY_HIDDEN)).next().get(0).asBoolean();
            }
          });
          // break;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public boolean updateLink(String dbid, Link l, boolean noinverse)
    throws Exception {
    // Retry logic is in addLink
    boolean added = addLink(dbid, l, noinverse);
    return !added; // return true if updated instead of added
  }


  // lookup using id1, type, id2
  @Override
  public Link getLink(String dbid, final long id1, final long link_type, final long id2)
      throws Exception {
    assert(false);
    final String statement = get_link_list_query;
    try (Session session = driver.session()) {

      while (true) { // retry for deadlock
        try {
          return session.readTransaction(new TransactionWork<Link>() {
            @Override 
            public Link execute(Transaction tx) {
              StatementResult sr = tx.run(statement, Values.parameters("id1", id1, "id2", id2));
              if (sr.hasNext() == false) {
                return null;
              }
              Value val = sr.single().get(0);
              Link l = new Link();
              l.id1 = id1;
              l.id2 = id2;
              l.link_type = link_type;
              l.data = val.get("data").asByteArray();
              l.time = val.get("time").asLong();
              l.version = val.get("version").asInt();
              l.visibility = (byte)val.get("visibility").asInt();
              return l;
            }
          });
          // break;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
  }
  @Override
  public Link[] multigetLinks(String dbid, final long id1, final long link_type,
                            final long[] id2s)
    throws Exception {
    final String statement = multiget_link_query.replace("$link_type", String.valueOf(link_type));
    try (Session session = driver.session()) {
      while (true) { // retry for deadlock
        try {
          return session.readTransaction(new TransactionWork<Link[]>() {
            @Override 
            public Link[] execute(Transaction tx) {
              List<Record> list = tx.run(statement, Values.parameters(
                  "id1", id1, 
                  "id2s", id2s)).list();
              if (list.size() == 0) return new Link[0];
              Link[] linklist = new Link[list.size()];

              for (int i = 0; i < list.size(); i++) {
                // Value val = tx.run(statement).next().get(0);
                Value val = list.get(i).get(0);
                Link l = new Link();
                l.id1 = id1;
                l.link_type = link_type;
                l.id2 = val.get("id2").asLong();
                l.data = val.get("data").asByteArray();
                l.time = val.get("time").asLong();
                l.version = val.get("version").asInt();
                l.visibility = (byte)val.get("visibility").asInt();
                linklist[i] = l;
              }
              return linklist;
            }
          });
          // break;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
  }

  // lookup using just id1, type
  @Override
  public Link[] getLinkList(String dbid, final long id1, final long link_type)
    throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }

  @Override
  public Link[] getLinkList(String dbid, final long id1, final long link_type,
                            final long minTimestamp, final long maxTimestamp,
                            final int offset, final int limit)
    throws Exception {
    final String statement = get_link_list_query.replace("$link_type", String.valueOf(link_type));
    try (Session session = driver.session()) {

      while (true) { // retry for deadlock
        try {
          return session.readTransaction(new TransactionWork<Link[]>() {
            @Override 
            public Link[] execute(Transaction tx) {
              List<Record> list = tx.run(statement, Values.parameters(
                  "id1", id1, 
                  "maxTimestamp", maxTimestamp, 
                  "minTimestamp", minTimestamp, 
                  "offset", offset, 
                  "limit", limit,
                  "vis", (int)VISIBILITY_DEFAULT)).list();
              if (list.size() == 0) return new Link[0];
              Link[] linklist = new Link[list.size()];

              for (int i = 0; i < list.size(); i++) {
                // Value val = tx.run(statement).next().get(0);
                Value val = list.get(i).get(0);
                Link l = new Link();
                l.id1 = id1;
                l.link_type = link_type;
                try {
                  l.id2 = val.get("id2").asLong();
                } catch (Exception e) {
                  return new Link[0];
                }
                l.data = val.get("data").asByteArray();
                l.time = val.get("time").asLong();
                l.version = val.get("version").asInt();
                // l.visibility = val.get("visibility").asByteArray()[0];
                l.visibility = (byte)val.get("visibility").asInt();
                linklist[i] = l;
              }
              return linklist;
            }
          });
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
  }

  // count the #links
  @Override
  public long countLinks(String dbid, final long id1, long link_type)
    throws Exception {
    final String statement = count_link_query.replace("$link_type", String.valueOf(link_type));
    try (Session session = driver.session()) {

      while (true) { // retry for deadlock
        try {
          return session.readTransaction(new TransactionWork<Integer>() {
            @Override 
            public Integer execute(Transaction tx) {
              return tx.run(statement, Values.parameters("id1", id1)).single().get(0).asInt();
            }
          });
          // break;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public int bulkLoadBatchSize() {
    return bulkInsertSize;
    // return 0;
  }

  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception {
  }

  @Override
  public void addBulkCounts(String dbid, List<LinkCount> a)
      throws Exception {}

  @Override
  public long addNode(String dbid, final Node node) throws Exception {
    final long allocatedID = _nodeid.getAndIncrement();
    /**
     * merge (a {id:0})
     * on create set a.data='hello', a.test=true
     * on match set  a.data='world'
     * with a, exists(a.test) as test 
     * remove a.test 
     * return test
     */

    final String insert = add_node_query;
    try (Session session = driver.session()) {
      while (true) { // retry for deadlock
        try {
          session.writeTransaction(new TransactionWork<Void>() {
            @Override 
            public Void execute(Transaction tx) {
              tx.run(insert, Values.parameters("id", allocatedID,
                                              "type", node.type,
                                              "time", node.time,
                                              "version", node.version,
                                              "data", stringLiteral(node.data)));
              return null;
            }
          });
          break;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
    return allocatedID;
  }

  @Override
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    long ids[] = new long[nodes.size()];
    StringBuilder sb = new StringBuilder();
    sb.append("UNWIND $props AS map CREATE (a:nt) SET a = map");

    List<Map<String,Object>> maps = new ArrayList<Map<String,Object>>();
    // Map<String,Object> params = new HashMap<>();
    // params.put("props", maps);
    for (int i = 0; i < nodes.size(); i++) {
      final Node n = nodes.get(i);
      final long id = _nodeid.getAndIncrement();
      ids[i] = id;
      maps.add(new HashMap<String, Object>() {{
        put("id", id); 
        put("type", n.type);
        put("time", n.time);
        put("version", n.version);
        put("data", stringLiteral(n.data));
        put("valid", true);
      }});
    }
    final String insert = sb.toString();
    final Map<String, Object> params = new HashMap<String, Object>();
    params.put("props", maps);
    try (Session session = driver.session()) {
      while (true) { // retry for deadlock
        try {
          session.writeTransaction(new TransactionWork<Void>() {
            @Override 
            public Void execute(Transaction tx) {
              tx.run(insert, params);
              return null;
            }
          });
          break;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
    return ids;
  }

  @Override
  public Node getNode(String dbid, final int type, final long id) throws Exception {
    final String statement = get_node_query;
    try (Session session = driver.session()) {
      while (true) { // retry for deadlock
        try {
          return session.readTransaction(new TransactionWork<Node>() {
            @Override 
            public Node execute(Transaction tx) {
              StatementResult sr = tx.run(statement, Values.parameters("id", id, "type", type));
              if (sr.hasNext() == false) return null;
              Value val = sr.single().get(0);
              Node n = new Node(id, val.get("type").asInt(), 
                                    val.get("version").asLong(), 
                                    val.get("time").asInt(), 
                                    val.get("data").asByteArray());
              return n;
            }
          });
          // break;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
  }


  @Override
  public boolean updateNode(String dbid, final Node node) throws Exception {
    /**
     * merge (a {id:0})
     * on create set a.data='hello', a.test=true
     * on match set  a.data='world'
     * with a, exists(a.test) as test 
     * remove a.test 
     * return test
     */
    long start_time = System.nanoTime();
    final String statement = update_node_query;
    try (Session session = driver.session()) {
      while (true) { // retry for deadlock
        try {
          int cnt;
          if (use_given_path) {

          cnt =  session.writeTransaction(new TransactionWork<Integer>() {
            @Override 
            public Integer execute(Transaction tx) {
              StatementResult sr = tx.run(statement, Values.parameters("node_list", AliPath.paths.get((int)(node.id))));
              if (sr.hasNext() == false) return null;
              return sr.single().get(0).asInt();
            }
          });

          } else {

          cnt =  session.writeTransaction(new TransactionWork<Integer>() {
            @Override 
            public Integer execute(Transaction tx) {
              StatementResult sr = tx.run(statement, Values.parameters("id", node.id,
                                                                      "type", node.type,
                                                                      "time", node.time,
                                                                      "version", node.version,
                                                                      "data", stringLiteral(node.data)));
              if (sr.hasNext() == false) return null;
              return sr.single().get(0).asInt();
            }
          });

          }
          // break;
          long end_time = System.nanoTime();
          _measurements.measure("count-time", cnt, (end_time - start_time)/1000);
          return cnt > 0;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public boolean deleteNode(String dbid, final int type, final long id) throws Exception {
    /**
     * merge (a {id:0})
     * on create set a.data='hello', a.test=true
     * on match set  a.data='world'
     * with a, exists(a.test) as test 
     * remove a.test 
     * return test
     */
    final String statement = delete_node_query;
    try (Session session = driver.session()) {
      while (true) { // retry for deadlock
        try {
          return session.writeTransaction(new TransactionWork<Boolean>() {
            @Override 
            public Boolean execute(Transaction tx) {
              StatementResult sr = tx.run(statement, Values.parameters("id", id, "type", type));
              if (sr.hasNext() == false) return null;
              return sr.single().get(0).asBoolean();
            }
          });
          // break;
        } catch (TransientException e) {
          retries += 1;
          continue; // retry for deadlock
        }
      } // end while, retry  for deadlock
    } catch (Exception e) {
      throw e;
    }
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
    // sb.append();
    for (int i = 0; i < cb.length(); i++) {
      char c = cb.get(i);
      switch (c) {
        // case '\'':
        //   sb.append("\\'");
        //   break;
        // case '\\':
        //   sb.append("\\\\");
        //   break;
        // case '\0':
        //   sb.append("\\0");
        //   break;
        // case '\b':
        //   sb.append("\\b");
        //   break;
        // case '\n':
        //   sb.append("\\n");
        //   break;
        // case '\r':
        //   sb.append("\\r");
        //   break;
        // case '\t':
        //   sb.append("\\t");
        //   break;
        // case '\f':
        //   sb.append("\\f");
        //   break;
        default:
          // if (Character.getNumericValue(c) < 0) {
          //   // Fall back on hex string for values not defined in latin-1
          //   return hexStringLiteral(arr);
          // } else {
            sb.append(c);
          // }
      }
    }
    // sb.append('\'');
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
    // sb.append("x'");
    for (int i = 0; i < arr.length; i++) {
      byte b = arr[i];
      int lo = b & 0xf;
      int hi = (b >> 4) & 0xf;
      sb.append(Character.forDigit(hi, 16));
      sb.append(Character.forDigit(lo, 16));
    }
    // sb.append("'");
    return sb.toString();
  }
  public static void main(String[] args) {
    String uri = "bolt://localhost:7687";
    Driver driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "admin"));
    try (Session session = driver.session()) {
      session.writeTransaction(new TransactionWork<Void>() {
        @Override 
        public Void execute(Transaction tx) {
          tx.run("CREATE CONSTRAINT ON (n:nt) ASSERT n.id IS UNIQUE");
          return null;
        }
      });
    } catch (Exception e) {
      throw e;
    }

    String dbid = "linkdb";
    final Node node;
    final long allocatedID = _nodeid.getAndIncrement();
    StringBuilder sb = new StringBuilder();
    /**
     * merge (a {id:0})
     * on create set a.data='hello', a.test=true
     * on match set  a.data='world'
     * with a, exists(a.test) as test 
     * remove a.test 
     * return test
     */

     
    sb.append("UNWIND $props AS map match (a:{id:0}), (b:{id:1}) CREATE (a)-[r]->(b) SET r = map");
    final String statement = sb.toString();
    try (Session session = driver.session()) {
      session.writeTransaction(new TransactionWork<Void>() {
        @Override 
        public Void execute(Transaction tx) {
          Map<String, Object> n1 = new HashMap<>();
          n1.put( "name", "Andy" );
          n1.put( "position", "Developer" );
          n1.put( "awesome", true );
          Map<String,Object> n2 = new HashMap<>();
          n2.put( "name", "Michael" );
          n2.put( "position", "Developer" );
          n2.put( "children", 3 );
          List<Map<String,Object>> maps = Arrays.asList( n1, n2 );
          Map<String,Object> params = new HashMap<>();
          params.put("props", maps);
          // param.add(new HashMap<String, Object>() {{put("id", 0);}});
          // param.add(new HashMap<String, Object>() {{put("id", 1);}});
          // param.add(new HashMap<String, Object>() {{put("id", 2);}});
          // tx.run(statement, Values.parameters(params));
          tx.run(statement, params);
          return null;
        }
      });
    } catch (Exception e) {
      throw e;
    }
  }

}

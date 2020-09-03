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
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Map;

import org.neo4j.driver.v1.exceptions.*;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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

  public static final int DEFAULT_BULKINSERT_SIZE = 1024;

  private static final boolean INTERNAL_TESTING = false;

  private static boolean _dbidready = false;
  private static String _dbid = "";
  private static String __sync = "";
  private static AtomicLong _nodeid = new AtomicLong(1);

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
  private Driver driver;
  // private Session session;

  private Phase phase;

  int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;
  // Optional optimization: disable binary logging
  // boolean disableBinLogForLoad = false;

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  public LinkStoreNeo4j() {
    super();
  }

  public LinkStoreNeo4j(Properties props) throws IOException, Exception {
    super();
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

  public void initialize(Properties props, Phase currentPhase,
    int threadId) throws IOException, Exception {
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

    // connect
    try {
      openConnection();
    } catch (Exception e) {
      logger.error("error connecting to database:", e);
      throw e;
    }

    // linktable = ConfigUtil.getPropertyRequired(props, Config.LINK_TABLE);
  }

  // connects to test database
  private void openConnection() throws Exception {

    String uri = "bolt://" + host + ":" + port;
    driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pwd));
    try (Session session = driver.session()) {
      session.writeTransaction(new TransactionWork<Void>() {
        @Override 
        public Void execute(Transaction tx) {
          // tx.run("CREATE INDEX ON :nt(id)");
          tx.run("CREATE CONSTRAINT ON (n:nt) ASSERT n.id IS UNIQUE");
          return null;
        }
      });
    } catch (Exception e) {
      throw e;
    }
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
    
    StringBuilder sb = new StringBuilder();
    /**
     * merge (a {id:0})
     * on create set a.data='hello', a.test=true
     * on match set  a.data='world'
     * with a, exists(a.test) as test 
     * remove a.test 
     * return test
     */
    // Long start = System.nanoTime();
    sb.append(" match (a:nt {id:$id1}), (b:nt {id : $id2}) \n");
    sb.append(" merge (a)-[r:`" + l.link_type + "`]->(b) \n");
    sb.append(" on match set r.covered=true\n");
    sb.append(" with r, exists(r.covered) as covered \n");
    // sb.append(" set r.id1=" + l.id1 + 
    //              ", r.type=" + l.link_type +
    //              ", r.id2=" + l.id2 +
    //              ", r.visibility= '" + l.visibility + "'" +
    //              ", r.data= " + stringLiteral(l.data) +
    //              ", r.time=" + l.time +
    //              ", r.version=" + l.version + "\n");
    sb.append(" set r.id1=$id1"                + 
                 ", r.type=$type"              +
                 ", r.id2=$id2"                +
                 ", r.visibility=$visibility " +
                 ", r.data=$data "             +
                 ", r.time=$time"              +
                 ", r.version=$version"        + "\n");

    sb.append(" remove r.covered \n return covered");
    final String insert = sb.toString();
    try (Session session = driver.session()) {
      // Long ts1 = System.nanoTime();
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
              "data", l.data,
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
        throw new Exception("Adding link, missing node1.id=" + l.id1 + " or 2.id=" + l.id2);
      }
      return ret.ret == 1;

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
    sb.append(" match (a:nt {id:$id1})-[r:`" + link_type + "`]->(b:nt {id : $id2}) ");
    if (expunge) {
      sb.append(" delete r");
    } else {
      sb.append(" set r.visibility=$vis");
    }
    sb.append(" return count(r)>0 ");

    final String insert = sb.toString();
    try (Session session = driver.session()) {
      return session.writeTransaction(new TransactionWork<Boolean>() {
        @Override 
        public Boolean execute(Transaction tx) {
          return tx.run(insert, Values.parameters("id1", id1, "id2", id2, "vis", (int)VISIBILITY_HIDDEN)).next().get(0).asBoolean();
        }
      });
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
    StringBuilder sb = new StringBuilder();
    sb.append(" match (a:nt {id:$id1})-[r:`" + link_type + "`]->(b:nt {id : $id2}) ");
    sb.append(" return r");
    
    final String statement = sb.toString();
    try (Session session = driver.session()) {
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
    } catch (Exception e) {
      throw e;
    }
  }
  @Override
  public Link[] multigetLinks(String dbid, final long id1, final long link_type,
                            final long[] id2s)
    throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append(" match (a:nt {id:$id1})-[r:`" + link_type + "`]->(b:nt) ");
    sb.append(" where b.id in $id2s " );
    sb.append(" return r");
    
    final String statement = sb.toString();
    try (Session session = driver.session()) {
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
    StringBuilder sb = new StringBuilder();
    sb.append(" match (a:nt {id:$id1})-[r:`" + link_type + "` {visibility : $vis}]->(b:nt) ");
    sb.append(" where r.time < $maxTimestamp and r.time > $minTimestamp" );
    sb.append(" return r ORDER BY r.time DESC ");
    sb.append(" skip $offset limit $limit");
    
    final String statement = sb.toString();
    try (Session session = driver.session()) {
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
            l.id2 = val.get("id2").asLong();
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
    } catch (Exception e) {
      throw e;
    }
  }

  // count the #links
  @Override
  public long countLinks(String dbid, final long id1, long link_type)
    throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append(" match (a:nt {id:$id1})-[r:`" + link_type + "`]->(b:nt) ");
    sb.append(" return count(r)");
    
    final String statement = sb.toString();
    try (Session session = driver.session()) {
      return session.readTransaction(new TransactionWork<Integer>() {
        @Override 
        public Integer execute(Transaction tx) {
          return tx.run(statement, Values.parameters("id1", id1)).single().get(0).asInt();
        }
      });
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public int bulkLoadBatchSize() {
    // return bulkInsertSize;
    return 0;
  }

  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception {
  }
  // private static long addnodecount = 0;

  @Override
  public long addNode(String dbid, final Node node) throws Exception {
    final long allocatedID = _nodeid.getAndIncrement();
    StringBuilder sb = new StringBuilder();
    // addnodecount ++;
    // if (addnodecount % 100 == 0) {
    //   System.err.println("added " + addnodecount + " nodes");
    // }
    /**
     * merge (a {id:0})
     * on create set a.data='hello', a.test=true
     * on match set  a.data='world'
     * with a, exists(a.test) as test 
     * remove a.test 
     * return test
     */

    sb.append(" create (a:nt {id : $id" +
                           ", valid : true" +
                           ", type : $type" +
                           ", time : $time" +
                           ", version : $version" +
                           ", data : $data})");

    // sb.append(" merge (a:nt {id:" + allocatedID + "}) ");
    // sb.append(" on create set a.id=" + allocatedID + 
    //                         ", a.type=" + node.type + 
    //                         ", a.created=true" + 
    //                         ", a.time=" + node.time +
    //                         ", a.version=" + node.version +
    //                         ", a.data=" + stringLiteral(node.data));
    // sb.append(" with a, exists(a.created) as created ");
    // sb.append(" remove a.created return created");
    final String insert = sb.toString();
    try (Session session = driver.session()) {
      session.writeTransaction(new TransactionWork<Void>() {
        @Override 
        public Void execute(Transaction tx) {
          tx.run(insert, Values.parameters("id", allocatedID,
                                           "type", node.type,
                                           "time", node.time,
                                           "version", node.version,
                                           "data", node.data));
          return null;
        }
      });
    } catch (Exception e) {
      throw e;
    }
    return allocatedID;
  }

  @Override
  public Node getNode(String dbid, final int type, final long id) throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append(" match (a:nt {id:$id, valid:true, type:$type}) ");
    sb.append(" return a");
    
    final String statement = sb.toString();
    try (Session session = driver.session()) {
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
    } catch (Exception e) {
      throw e;
    }
  }


  @Override
  public boolean updateNode(String dbid, final Node node) throws Exception {
    StringBuilder sb = new StringBuilder();
    /**
     * merge (a {id:0})
     * on create set a.data='hello', a.test=true
     * on match set  a.data='world'
     * with a, exists(a.test) as test 
     * remove a.test 
     * return test
     */
    sb.append(" match (a:nt {id:$id, valid:true, type:$type}) ");
    sb.append(" set a.id=$id" + 
                 ", a.type=$type" + 
                 ", a.time=$time" + 
                 ", a.version=$version" + 
                 ", a.data=$data ");
    sb.append(" return count(a) > 0 ");
    final String statement = sb.toString();
    try (Session session = driver.session()) {
      return session.writeTransaction(new TransactionWork<Boolean>() {
        @Override 
        public Boolean execute(Transaction tx) {
          StatementResult sr = tx.run(statement, Values.parameters("id", node.id,
                                                                   "type", node.type,
                                                                   "time", node.time,
                                                                   "version", node.version,
                                                                   "data", node.data));
          if (sr.hasNext() == false) return null;
          return sr.single().get(0).asBoolean();
        }
      });
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public boolean deleteNode(String dbid, final int type, final long id) throws Exception {
    StringBuilder sb = new StringBuilder();
    /**
     * merge (a {id:0})
     * on create set a.data='hello', a.test=true
     * on match set  a.data='world'
     * with a, exists(a.test) as test 
     * remove a.test 
     * return test
     */
    sb.append(" match (a:nt {id:$id, valid:true, type:$type}) ");
    sb.append(" set a.valid=false return count(a)>0 " );
    final String statement = sb.toString();
    try (Session session = driver.session()) {
      return session.writeTransaction(new TransactionWork<Boolean>() {
        @Override 
        public Boolean execute(Transaction tx) {
          StatementResult sr = tx.run(statement, Values.parameters("id", id, "type", type));
          if (sr.hasNext() == false) return null;
          return sr.single().get(0).asBoolean();
        }
      });
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
    sb.append('\'');
    for (int i = 0; i < cb.length(); i++) {
      char c = cb.get(i);
      switch (c) {
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
    sb.append('\'');
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
    sb.append("x'");
    for (int i = 0; i < arr.length; i++) {
      byte b = arr[i];
      int lo = b & 0xf;
      int hi = (b >> 4) & 0xf;
      sb.append(Character.forDigit(hi, 16));
      sb.append(Character.forDigit(lo, 16));
    }
    sb.append("'");
    return sb.toString();
  }
}

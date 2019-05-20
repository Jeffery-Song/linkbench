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
    // counttable = ConfigUtil.getPropertyRequired(props, Config.COUNT_TABLE);
    // if (counttable.equals("")) {
    //   String msg = "Error! " + Config.COUNT_TABLE + " is empty!"
    //       + "Please check configuration file.";
    //   logger.error(msg);
    //   throw new RuntimeException(msg);
    // }

    // nodetable = props.getProperty(Config.NODE_TABLE);
    // if (nodetable.equals("")) {
    //   // For now, don't assume that nodetable is provided
    //   String msg = "Error! " + Config.NODE_TABLE + " is empty!"
    //       + "Please check configuration file.";
    //   logger.error(msg);
    //   throw new RuntimeException(msg);
    // }

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
    // if (props.containsKey(CONFIG_DISABLE_BINLOG_LOAD)) {
    //   disableBinLogForLoad = ConfigUtil.getBool(props,
    //                                   CONFIG_DISABLE_BINLOG_LOAD);
    // }

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
    // session = driver.session();

    // conn_ro = null;
    // conn_rw = null;
    // stmt_ro = null;
    // stmt_rw = null;
    // Random rng = new Random();

    // if (defaultDB != null) {
    //   jdbcUrl += defaultDB;
    // }

    // Class.forName("com.mysql.jdbc.Driver").newInstance();

  //   jdbcUrl += "?elideSetAutoCommits=true" +
  //              "&useLocalTransactionState=true" +
  //              "&allowMultiQueries=true" +
  //              "&useLocalSessionState=true" +
  //  /* Need affected row count from queries to distinguish updates/inserts
  //   * consistently across different MySql versions (see MySql bug 46675) */
  //              "&useAffectedRows=true";

    /* Fix for failing connections at high concurrency, short random delay for
     * each */
    // try {
    //   int t = rng.nextInt(1000) + 100;
    //   //System.err.println("Sleeping " + t + " msecs");
    //   Thread.sleep(t);
    // } catch (InterruptedException ie) {
    // }

    // conn_rw = DriverManager.getConnection(jdbcUrl, user, pwd);
    // conn_rw.setAutoCommit(false);

    // try {
    //   int t = rng.nextInt(1000) + 100;
    //   //System.err.println("Sleeping " + t + " msecs");
    //   Thread.sleep(t);
    // } catch (InterruptedException ie) {
    // }

    // conn_ro = DriverManager.getConnection(jdbcUrl, user, pwd);
    // conn_ro.setAutoCommit(true);

    // //System.err.println("connected");
    // stmt_rw = conn_rw.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
    //                                   ResultSet.CONCUR_READ_ONLY);
    // stmt_ro = conn_ro.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
    //                                   ResultSet.CONCUR_READ_ONLY);

    // if (phase == Phase.LOAD && disableBinLogForLoad) {
    //   // Turn binary logging off for duration of connection
    //   stmt_rw.executeUpdate("SET SESSION sql_log_bin=0");
    //   stmt_ro.executeUpdate("SET SESSION sql_log_bin=0");
    // }
  }

  @Override
  public void close() {
    // try {
    //   if (stmt_rw != null) stmt_rw.close();
    //   if (stmt_ro != null) stmt_ro.close();
    //   if (conn_rw != null) conn_rw.close();
    //   if (conn_ro != null) conn_ro.close();
    // } catch (Neo4jException e) {
    //   logger.error("Error while closing MySQL connection: ", e);
    // }
    driver.close();
  }

  public void clearErrors(int threadID) {
    // logger.info("Reopening MySQL connection in threadID " + threadID);

    // try {
    //   if (conn_rw != null) {
    //     conn_rw.close();
    //   }
    //   if (conn_ro != null) {
    //     conn_ro.close();
    //   }

    //   openConnection();
    // } catch (Throwable e) {
    //   e.printStackTrace();
    //   return;
    // }
  }
  // TODO: adapt the sql retry machenism;
  /**
   * Set of all JDBC SQLState strings that indicate a transient MySQL error
   * that should be handled by retrying
   */
  // private static final HashSet<String> retrySQLStates = populateRetrySQLStates();

  /**
   *  Populate retrySQLStates
   *  SQLState codes are defined in MySQL Connector/J documentation:
   *  http://dev.mysql.com/doc/refman/5.6/en/connector-j-reference-error-sqlstates.html
   */
  // private static HashSet<String> populateRetrySQLStates() {
  //   HashSet<String> states = new HashSet<String>();
  //   states.add("41000"); // ER_LOCK_WAIT_TIMEOUT
  //   states.add("40001"); // ER_LOCK_DEADLOCK
  //   return states;
  // }

  /**
   * Handle SQL exception by logging error and selecting how to respond
   * @param ex Neo4jException thrown by MySQL JDBC driver
   * @return true if transaction should be retried
   */
  private boolean processNeo4jException(Neo4jException ex, String op) {
    // boolean retry = retrySQLStates.contains(ex.getSQLState());
    // String msg = "Neo4jException thrown by MySQL driver during execution of " +
    //              "operation: " + op + ".  ";
    // msg += "Message was: '" + ex.getMessage() + "'.  ";
    // msg += "SQLState was: " + ex.getSQLState() + ".  ";

    // if (retry) {
    //   msg += "Error is probably transient, retrying operation.";
    //   logger.warn(msg);
    // } else {
    //   msg += "Error is probably non-transient, will abort operation.";
    //   logger.error(msg);
    // }
    // return retry;
    return false;
  }

  // get count for testing purpose
  /* private void testCount(Statement stmt, String dbid,
                         String assoctable, String counttable,
                         long id, long link_type)
    throws Exception {

    String select1 = "SELECT COUNT(id2)" +
                     " FROM " + dbid + "." + assoctable +
                     " WHERE id1 = " + id +
                     " AND link_type = " + link_type +
                     " AND visibility = " + LinkStore.VISIBILITY_DEFAULT;

    String select2 = "SELECT COALESCE (SUM(count), 0)" +
                     " FROM " + dbid + "." + counttable +
                     " WHERE id = " + id +
                     " AND link_type = " + link_type;

    String verify = "SELECT IF ((" + select1 +
                    ") = (" + select2 + "), 1, 0) as result";

    ResultSet result = stmt.executeQuery(verify);

    int ret = -1;
    while (result.next()) {
      ret = result.getInt("result");
    }

    if (ret != 1) {
      throw new Exception("Data inconsistency between " + assoctable +
                          " and " + counttable);
    }
  } */

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

    // while (true) {
    //   try {
    //     return addLinkImpl(dbid, l, noinverse);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "addLink")) {
    //       throw ex;
    //     }
    //   }
    // }
  }
  
  /* private boolean addLinkImpl(String dbid, Link l, boolean noinverse)
      throws Exception {

     if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("addLink " + l.id1 +
                         "." + l.id2 +
                         "." + l.link_type);
    }

    // if the link is already there then update its visibility
    // only update visibility; skip updating time, version, etc.

    int nrows = addLinksNoCount(dbid, Collections.singletonList(l));

    // Note: at this point, we have an exclusive lock on the link
    // row until the end of the transaction, so can safely do
    // further updates without concurrency issues.

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("nrows = " + nrows);
    }

    // based on nrows, determine whether the previous query was an insert
    // or update
    boolean row_found;
    boolean update_data = false;
    int update_count = 0;

    switch (nrows) {
      case 1:
        // a new row was inserted --> need to update counttable
        if (l.visibility == VISIBILITY_DEFAULT) {
          update_count = 1;
        }
        row_found = false;
        break;

      case 0:
        // A row is found but its visibility was unchanged
        // --> need to update other data
        update_data = true;
        row_found = true;
        break;

      case 2:
        // a visibility was changed from VISIBILITY_HIDDEN to DEFAULT
        // or vice-versa
        // --> need to update both counttable and other data
        if (l.visibility == VISIBILITY_DEFAULT) {
          update_count = 1;
        } else {
          update_count = -1;
        }
        update_data = true;
        row_found = true;
        break;

      default:
        String msg = "Value of affected-rows number is not valid" + nrows;
        logger.error("SQL Error: " + msg);
        throw new Exception(msg);
    }

    if (update_count != 0) {
      int base_count = update_count < 0 ? 0 : 1;
      // query to update counttable
      // if (id, link_type) is not there yet, add a new record with count = 1
      // The update happens atomically, with the latest count and version
      long currentTime = (new Date()).getTime();
      String updatecount = "INSERT INTO " + dbid + "." + counttable +
                      "(id, link_type, count, time, version) " +
                      "VALUES (" + l.id1 +
                      ", " + l.link_type +
                      ", " + base_count +
                      ", " + currentTime +
                      ", " + 0 + ") " +
                      "ON DUPLICATE KEY UPDATE" +
                      " count = count + " + update_count +
                      ", version = version + 1 " +
                      ", time = " + currentTime + ";";

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(updatecount);
      }

      // This is the last statement of transaction - append commit to avoid
      // extra round trip
      if (!update_data) {
        updatecount += " commit;";
      }
      stmt_rw.executeUpdate(updatecount);
    }

    if (update_data) {
      // query to update link data (the first query only updates visibility)
      String updatedata = "UPDATE " + dbid + "." + linktable + " SET" +
                  " visibility = " + l.visibility +
                  ", data = " +  stringLiteral(l.data)+
                  ", time = " + l.time +
                  ", version = " + l.version +
                  " WHERE id1 = " + l.id1 +
                  " AND id2 = " + l.id2 +
                  " AND link_type = " + l.link_type + "; commit;";
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(updatedata);
      }

      stmt_rw.executeUpdate(updatedata);
    }

    if (INTERNAL_TESTING) {
      testCount(stmt_ro, dbid, linktable, counttable, l.id1, l.link_type);
    }
    return row_found;
  } */

  /**
   * Internal method: add links without updating the count
   * @param dbid
   * @param links
   * @return
   * @throws Neo4jException
   */
  /* private int addLinksNoCount(String dbid, List<Link> links)
      throws Neo4jException {
    if (links.size() == 0)
      return 0;

    // query to insert a link;
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO " + dbid + "." + linktable +
                    "(id1, id2, link_type, " +
                    "visibility, data, time, version) VALUES ");
    boolean first = true;
    for (Link l : links) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      sb.append("(" + l.id1 +
          ", " + l.id2 +
          ", " + l.link_type +
          ", " + l.visibility +
          ", " + stringLiteral(l.data) +
          ", " + l.time + ", " +
          l.version + ")");
    }
    sb.append(" ON DUPLICATE KEY UPDATE visibility = VALUES(visibility)");
    String insert = sb.toString();
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(insert);
    }

    int nrows = stmt_rw.executeUpdate(insert);
    return nrows;
  } */

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
    // while (true) {
    //   try {
    //     return deleteLinkImpl(dbid, id1, link_type, id2, noinverse, expunge);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "deleteLink")) {
    //       throw ex;
    //     }
    //   }
    // }
  }

  /* private boolean deleteLinkImpl(String dbid, long id1, long link_type, long id2,
      boolean noinverse, boolean expunge) throws Exception {
    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("deleteLink " + id1 +
                         "." + id2 +
                         "." + link_type);
    }

    // First do a select to check if the link is not there, is there and
    // hidden, or is there and visible;
    // Result could be either NULL, VISIBILITY_HIDDEN or VISIBILITY_DEFAULT.
    // In case of VISIBILITY_DEFAULT, later we need to mark the link as
    // hidden, and update counttable.
    // We lock the row exclusively because we rely on getting the correct
    // value of visible to maintain link counts.  Without the lock,
    // a concurrent transaction could also see the link as visible and
    // we would double-decrement the link count.
    String select = "SELECT visibility" +
                    " FROM " + dbid + "." + linktable +
                    " WHERE id1 = " + id1 +
                    " AND id2 = " + id2 +
                    " AND link_type = " + link_type +
                    " FOR UPDATE;";

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(select);
    }

    ResultSet result = stmt_rw.executeQuery(select);

    int visibility = -1;
    boolean found = false;
    while (result.next()) {
      visibility = result.getInt("visibility");
      found = true;
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(String.format("(%d, %d, %d) visibility = %d",
                id1, link_type, id2, visibility));
    }

    if (!found) {
      // do nothing
    }
    else if (visibility == VISIBILITY_HIDDEN && !expunge) {
      // do nothing
    }
    else {
      // Only update count if link is present and visible
      boolean updateCount = (visibility != VISIBILITY_HIDDEN);

      // either delete or mark the link as hidden
      String delete;

      if (!expunge) {
        delete = "UPDATE " + dbid + "." + linktable +
                 " SET visibility = " + VISIBILITY_HIDDEN +
                 " WHERE id1 = " + id1 +
                 " AND id2 = " + id2 +
                 " AND link_type = " + link_type + ";";
      } else {
        delete = "DELETE FROM " + dbid + "." + linktable +
                 " WHERE id1 = " + id1 +
                 " AND id2 = " + id2 +
                 " AND link_type = " + link_type + ";";
      }

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(delete);
      }

      stmt_rw.executeUpdate(delete);

      // update count table
      // * if found (id1, link_type) in count table, set
      //   count = (count == 1) ? 0) we decrease the value of count
      //   column by 1;
      // * otherwise, insert new link with count column = 0
      // The update happens atomically, with the latest count and version
      long currentTime = (new Date()).getTime();
      String update = "INSERT INTO " + dbid + "." + counttable +
                      " (id, link_type, count, time, version) " +
                      "VALUES (" + id1 +
                      ", " + link_type +
                      ", 0" +
                      ", " + currentTime +
                      ", " + 0 + ") " +
                      "ON DUPLICATE KEY UPDATE" +
                      " count = IF (count = 0, 0, count - 1)" +
                      ", time = " + currentTime +
                      ", version = version + 1;";

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(update);
      }

      stmt_rw.executeUpdate(update);
    }

    conn_rw.commit();

    if (INTERNAL_TESTING) {
      testCount(stmt_ro, dbid, linktable, counttable, id1, link_type);
    }

    return found;
  } */

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
    // while (true) {
    //   try {
    //     return getLinkImpl(dbid, id1, link_type, id2);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "getLink")) {
    //       throw ex;
    //     }
    //   }
    // }
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
    // while (true) {
    //   try {
    //     return getLinkListImpl(dbid, id1, link_type, minTimestamp,
    //                            maxTimestamp, offset, limit);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "getLinkListImpl")) {
    //       throw ex;
    //     }
    //   }
    // }
  }

  // private Link getLinkImpl(String dbid, long id1, long link_type, long id2)
  //   throws Exception {
  //   Link res[] = multigetLinks(dbid, id1, link_type, new long[] {id2});
  //   if (res == null) return null;
  //   assert(res.length <= 1);
  //   return res.length == 0 ? null : res[0];
  // }


  // @Override
  // public Link[] multigetLinks(String dbid, long id1, long link_type,
  //                             long[] id2s) throws Exception {
  //   while (true) {
  //     try {
  //       return multigetLinksImpl(dbid, id1, link_type, id2s);
  //     } catch (Neo4jException ex) {
  //       if (!processNeo4jException(ex, "multigetLinks")) {
  //         throw ex;
  //       }
  //     }
  //   }
  // }

  // private Link[] multigetLinksImpl(String dbid, long id1, long link_type,
  //                               long[] id2s) throws Exception {
  //   StringBuilder querySB = new StringBuilder();
  //   querySB.append(" select id1, id2, link_type," +
  //       " visibility, data, time, " +
  //       " version from " + dbid + "." + linktable +
  //       " where id1 = " + id1 + " and link_type = " + link_type +
  //       " and id2 in (");
  //   boolean first = true;
  //   for (long id2: id2s) {
  //     if (first) {
  //       first = false;
  //     } else {
  //       querySB.append(",");
  //     }
  //     querySB.append(id2);
  //   }

  //   querySB.append(");");
  //   String query = querySB.toString();

  //   if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //     logger.trace("Query is " + query);
  //   }

  //   ResultSet rs = stmt_ro.executeQuery(query);

  //   // Get the row count to allocate result array
  //   assert(rs.getType() != ResultSet.TYPE_FORWARD_ONLY);
  //   rs.last();
  //   int count = rs.getRow();
  //   rs.beforeFirst();

  //   Link results[] = new Link[count];
  //   int i = 0;
  //   while (rs.next()) {
  //     Link l = createLinkFromRow(rs);
  //     if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //       logger.trace("Lookup result: " + id1 + "," + link_type + "," +
  //                 l.id2 + " found");
  //     }
  //     results[i++] = l;
  //   }
  //   return results;
  // }

  // lookup using just id1, type
  @Override
  public Link[] getLinkList(String dbid, final long id1, final long link_type)
    throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
    // StringBuilder sb = new StringBuilder();
    // sb.append(" match (a:nt {id:$id1})-[r:`" + link_type + "` {visibility : $vis}]->(b:nt) ");
    // sb.append(" return r ORDER BY r.time DESC");
    
    // final String statement = sb.toString();
    // try (Session session = driver.session()) {
    //   return session.readTransaction(new TransactionWork<Link[]>() {
    //     @Override 
    //     public Link[] execute(Transaction tx) {
    //       List<Record> list = tx.run(statement, Values.parameters("id1", id1, "vis", VISIBILITY_DEFAULT)).list();
    //       if (list.size() == 0) return new Link[0];
    //       Link[] linklist = new Link[list.size()];

    //       for (int i = 0; i < list.size(); i++) {
    //         // Value val = tx.run(statement).next().get(0);
    //         Value val = list.get(i).get(0);
    //         Link l = new Link();
    //         l.id1 = id1;
    //         l.link_type = link_type;
    //         l.id2 = val.get("id2").asLong();
    //         l.data = val.get("data").asByteArray();
    //         l.time = val.get("time").asLong();
    //         l.version = val.get("version").asInt();
    //         l.visibility = (byte)val.get("visibility").asInt();
    //         linklist[i] = l;
    //       }
    //       return linklist;
    //     }
    //   });
    // } catch (Exception e) {
    //   throw e;
    // }
    // Retry logic in getLinkList
    // return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
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
    // while (true) {
    //   try {
    //     return getLinkListImpl(dbid, id1, link_type, minTimestamp,
    //                            maxTimestamp, offset, limit);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "getLinkListImpl")) {
    //       throw ex;
    //     }
    //   }
    // }
  }

  // private Link[] getLinkListImpl(String dbid, long id1, long link_type,
  //       long minTimestamp, long maxTimestamp,
  //       int offset, int limit)
  //           throws Exception {
  //   String query = " select id1, id2, link_type," +
  //                  " visibility, data, time," +
  //                  " version from " + dbid + "." + linktable +
  //                  " FORCE INDEX(`id1_type`) " +
  //                  " where id1 = " + id1 + " and link_type = " + link_type +
  //                  " and time >= " + minTimestamp +
  //                  " and time <= " + maxTimestamp +
  //                  " and visibility = " + LinkStore.VISIBILITY_DEFAULT +
  //                  " order by time desc " +
  //                  " limit " + offset + "," + limit + ";";

  //   if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //     logger.trace("Query is " + query);
  //   }

  //   ResultSet rs = stmt_ro.executeQuery(query);

  //   // Find result set size
  //   // be sure we fast forward to find result set size
  //   assert(rs.getType() != ResultSet.TYPE_FORWARD_ONLY);
  //   rs.last();
  //   int count = rs.getRow();
  //   rs.beforeFirst();

  //   if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //     logger.trace("Range lookup result: " + id1 + "," + link_type +
  //                        " is " + count);
  //   }
  //   if (count == 0) {
  //     return null;
  //   }

  //   // Fetch the link data
  //   Link links[] = new Link[count];
  //   int i = 0;
  //   while (rs.next()) {
  //     Link l = createLinkFromRow(rs);
  //     links[i] = l;
  //     i++;
  //   }
  //   assert(i == count);
  //   return links;
  // }

  // private Link createLinkFromRow(ResultSet rs) throws Neo4jException {
  //   Link l = new Link();
  //   l.id1 = rs.getLong(1);
  //   l.id2 = rs.getLong(2);
  //   l.link_type = rs.getLong(3);
  //   l.visibility = rs.getByte(4);
  //   l.data = rs.getBytes(5);
  //   l.time = rs.getLong(6);
  //   l.version = rs.getInt(7);
  //   return l;
  // }

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
    
    
    // while (true) {
    //   try {
    //     return countLinksImpl(dbid, id1, link_type);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "countLinks")) {
    //       throw ex;
    //     }
    //   }
    // }
  }

  // private long countLinksImpl(String dbid, long id1, long link_type)
  //       throws Exception {
  //   long count = 0;
  //   String query = " select count from " + dbid + "." + counttable +
  //                  " where id = " + id1 + " and link_type = " + link_type + ";";

  //   ResultSet rs = stmt_ro.executeQuery(query);
  //   boolean found = false;

  //   while (rs.next()) {
  //     // found
  //     if (found) {
  //       logger.trace("Count query 2nd row!: " + id1 + "," + link_type);
  //     }

  //     found = true;
  //     count = rs.getLong(1);
  //   }

  //   if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //     logger.trace("Count result: " + id1 + "," + link_type +
  //                        " is " + found + " and " + count);
  //   }

  //   return count;
  // }

  @Override
  public int bulkLoadBatchSize() {
    // return bulkInsertSize;
    return 0;
  }

  // @Override
  // public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
  //     throws Exception {
  //   while (true) {
  //     try {
  //       addBulkLinksImpl(dbid, links, noinverse);
  //       return;
  //     } catch (Neo4jException ex) {
  //       if (!processNeo4jException(ex, "addBulkLinks")) {
  //         throw ex;
  //       }
  //     }
  //   }
  // }

  // private void addBulkLinksImpl(String dbid, List<Link> links, boolean noinverse)
  //     throws Exception {
  //   if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //     logger.trace("addBulkLinks: " + links.size() + " links");
  //   }

  //   addLinksNoCount(dbid, links);
  //   conn_rw.commit();
  // }

  // @Override
  // public void addBulkCounts(String dbid, List<LinkCount> counts)
  //                                               throws Exception {
  //   while (true) {
  //     try {
  //       addBulkCountsImpl(dbid, counts);
  //       return;
  //     } catch (Neo4jException ex) {
  //       if (!processNeo4jException(ex, "addBulkCounts")) {
  //         throw ex;
  //       }
  //     }
  //   }
  // }

  // private void addBulkCountsImpl(String dbid, List<LinkCount> counts)
  //                                               throws Exception {
  //   if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //     logger.trace("addBulkCounts: " + counts.size() + " link counts");
  //   }
  //   if (counts.size() == 0)
  //     return;

  //   StringBuilder sqlSB = new StringBuilder();
  //   sqlSB.append("REPLACE INTO " + dbid + "." + counttable +
  //       "(id, link_type, count, time, version) " +
  //       "VALUES ");
  //   boolean first = true;
  //   for (LinkCount count: counts) {
  //     if (first) {
  //       first = false;
  //     } else {
  //       sqlSB.append(",");
  //     }
  //     sqlSB.append("(" + count.id1 +
  //       ", " + count.link_type +
  //       ", " + count.count +
  //       ", " + count.time +
  //       ", " + count.version + ")");
  //   }

  //   String sql = sqlSB.toString();
  //   if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //     logger.trace(sql);
  //   }
  //   stmt_rw.executeUpdate(sql);
  //   conn_rw.commit();
  // }

  // private void checkNodeTableConfigured() throws Exception {
  //   if (this.nodetable == null) {
  //     throw new Exception("Nodetable not specified: cannot perform node" +
  //         " operation");
  //   }
  // }

  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception {
    // checkNodeTableConfigured();
    // // Truncate table deletes all data and allows us to reset autoincrement
    // stmt_rw.execute(String.format("TRUNCATE TABLE `%s`.`%s`;",
    //              dbid, nodetable));
    // stmt_rw.execute(String.format("ALTER TABLE `%s`.`%s` " +
    //     "AUTO_INCREMENT = %d;", dbid, nodetable, startID));
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
    // while (true) {
    //   try {
    //     return addNodeImpl(dbid, node);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "addNode")) {
    //       throw ex;
    //     }
    //   }
    // }
  }

  // private long addNodeImpl(String dbid, Node node) throws Exception {
  //   long ids[] = bulkAddNodes(dbid, Collections.singletonList(node));
  //   assert(ids.length == 1);
  //   return ids[0];
  // }

  // @Override
  // public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
  //   while (true) {
  //     try {
  //       return bulkAddNodesImpl(dbid, nodes);
  //     } catch (Neo4jException ex) {
  //       if (!processNeo4jException(ex, "bulkAddNodes")) {
  //         throw ex;
  //       }
  //     }
  //   }
  // }

  // private long[] bulkAddNodesImpl(String dbid, List<Node> nodes) throws Exception {
  //   checkNodeTableConfigured();
  //   StringBuilder sql = new StringBuilder();
  //   sql.append("INSERT INTO `" + dbid + "`.`" + nodetable + "` " +
  //       "(type, version, time, data) " +
  //       "VALUES ");
  //   boolean first = true;
  //   for (Node node: nodes) {
  //     if (first) {
  //       first = false;
  //     } else {
  //       sql.append(",");
  //     }
  //     sql.append("(" + node.type + "," + node.version +
  //         "," + node.time + "," + stringLiteral(node.data) + ")");
  //   }
  //   sql.append("; commit;");
  //   if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //     logger.trace(sql);
  //   }
  //   stmt_rw.executeUpdate(sql.toString(), Statement.RETURN_GENERATED_KEYS);
  //   ResultSet rs = stmt_rw.getGeneratedKeys();

  //   long newIds[] = new long[nodes.size()];
  //   // Find the generated id
  //   int i = 0;
  //   while (rs.next() && i < nodes.size()) {
  //     newIds[i++] = rs.getLong(1);
  //   }

  //   if (i != nodes.size()) {
  //     throw new Exception("Wrong number of generated keys on insert: "
  //         + " expected " + nodes.size() + " actual " + i);
  //   }

  //   assert(!rs.next()); // check done
  //   rs.close();

  //   return newIds;
  // }

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
    // while (true) {
    //   try {
    //     return getNodeImpl(dbid, type, id);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "getNode")) {
    //       throw ex;
    //     }
    //   }
    // }
  }

  // private Node getNodeImpl(String dbid, int type, long id) throws Exception {
  //   checkNodeTableConfigured();
  //   ResultSet rs = stmt_ro.executeQuery(
  //     "SELECT id, type, version, time, data " +
  //     "FROM `" + dbid + "`.`" + nodetable + "` " +
  //     "WHERE id=" + id + ";");
  //   if (rs.next()) {
  //     Node res = new Node(rs.getLong(1), rs.getInt(2),
  //          rs.getLong(3), rs.getInt(4), rs.getBytes(5));

  //     // Check that multiple rows weren't returned
  //     assert(rs.next() == false);
  //     rs.close();
  //     if (res.type != type) {
  //       return null;
  //     } else {
  //       return res;
  //     }
  //   }
  //   return null;
  // }

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
    // return false;
    // while (true) {
    //   try {
    //     return updateNodeImpl(dbid, node);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "updateNode")) {
    //       throw ex;
    //     }
    //   }
    // }
  }

  // private boolean updateNodeImpl(String dbid, Node node) throws Exception {
  //   checkNodeTableConfigured();
  //   String sql = "UPDATE `" + dbid + "`.`" + nodetable + "`" +
  //           " SET " + "version=" + node.version + ", time=" + node.time
  //                  + ", data=" + stringLiteral(node.data) +
  //           " WHERE id=" + node.id + " AND type=" + node.type + "; commit;";

  //   if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
  //     logger.trace(sql);
  //   }

  //   int rows = stmt_rw.executeUpdate(sql);

  //   if (rows == 1) return true;
  //   else if (rows == 0) return false;
  //   else throw new Exception("Did not expect " + rows +  "affected rows: only "
  //       + "expected update to affect at most one row");
  // }

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
    // while (true) {
    //   try {
    //     return deleteNodeImpl(dbid, type, id);
    //   } catch (Neo4jException ex) {
    //     if (!processNeo4jException(ex, "deleteNode")) {
    //       throw ex;
    //     }
    //   }
    // }
  }

  // private boolean deleteNodeImpl(String dbid, int type, long id) throws Exception {
  //   StringBuilder sb = new StringBuilder();
  //   /**
  //    * merge (a {id:0})
  //    * on create set a.data='hello', a.test=true
  //    * on match set  a.data='world'
  //    * with a, exists(a.test) as test 
  //    * remove a.test 
  //    * return test
  //    */
  //   sb.append(" match (a {id:" + node.id + ", type:" + node.type + "}) ");
  //   sb.append(" set a.id=" + node.id + 
  //                ", a.type=" + node.type +
  //                ", a.time=" + node.time +
  //                ", a.version=" + node.version + 
  //                ", a.data='" + stringLiteral(node.data) +"'" );
  //   final String insert = sb.toString();
  //   try (Session session = driver.session()) {
  //     session.writeTransaction(new TransactionWork<Void>() {
  //       @Override 
  //       public Void execute(Transaction tx) {
  //         tx.run(insert);
  //         return null;
  //       }
  //     });
  //     return true;
  //   } catch (Exception e) {
  //     throw e;
  //   }
  //   checkNodeTableConfigured();
  //   int rows = stmt_rw.executeUpdate(
  //       "DELETE FROM `" + dbid + "`.`" + nodetable + "` " +
  //       "WHERE id=" + id + " and type =" + type + "; commit;");

  //   if (rows == 0) {
  //     return false;
  //   } else if (rows == 1) {
  //     return true;
  //   } else {
  //     throw new Exception(rows + " rows modified on delete: should delete " +
  //                     "at most one");
  //   }
  // }

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
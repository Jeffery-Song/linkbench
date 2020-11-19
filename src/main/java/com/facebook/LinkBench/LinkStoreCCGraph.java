package com.facebook.LinkBench;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Properties;
import java.io.IOException;

import com.facebook.LinkBench.GraphStore;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;

import com.google.protobuf.ByteString;

import com.ccgraph.ccgraph.CCGraphServerGrpc;
import com.ccgraph.ccgraph.StartParam;
// import com.ccgraph.ccgraph.CodeRet;
import com.ccgraph.ccgraph.CallParam;
import com.ccgraph.ccgraph.Code;
import com.ccgraph.ccgraph.CommandParam;
import com.ccgraph.ccgraph.Results;
import com.ccgraph.ccgraph.RetRow;

import com.facebook.LinkBench.measurements.Measurements;

/**
 * A simple client that requests a greeting from the {@link HelloWorldServer}.
 */
public class LinkStoreCCGraph extends GraphStore {
  private static final Logger logger = Logger.getLogger(LinkStoreCCGraph.class.getName());

  private static ManagedChannel channel = null;
  private static String __channel_sync = "";
  private CCGraphServerGrpc.CCGraphServerBlockingStub blockingStub;

  private static boolean _dbidready = false;
  private static String _dbid = "";
  private static String __sync = "";
  private static AtomicLong _nodeid;
  private Measurements _measurements = Measurements.getMeasurements();
  private boolean is_ali = true;
  private boolean ali_given_path = false;
  
  /** Construct client connecting to HelloWorld server at {@code host:port}. */
  // public LinkStoreCCGraph(String host, int port) {
  //   this(ManagedChannelBuilder.forAddress(host, port)
  //       // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
  //       // needing certificates.
  //       .usePlaintext()
  //       .build());
  // }

  public void initialize(Properties p,
      Phase currentPhase, int threadId) throws IOException, Exception {
    if (currentPhase == Phase.LOAD) {
      _nodeid = new AtomicLong(1);
    } else {
      // long maxId = ConfigUtil.getLong(props, Config.MAX_ID);
      _nodeid = new AtomicLong(ConfigUtil.getLong(p, com.facebook.LinkBench.Config.MAX_ID));
    }
    ali_given_path = ConfigUtil.getBool(p, "ali.request_via_explicit_path", false);
    if (ali_given_path == true) {
      System.err.println("ali: login path is given");
    } else {
      System.err.println("ali: login path is not given");
    }
    String host = p.getProperty("host", "127.0.0.1");
    int port = Integer.parseInt(p.getProperty("port", "55555"));
    synchronized(__channel_sync) {
      if (channel == null) 
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
      this.blockingStub = CCGraphServerGrpc.newBlockingStub(channel);
    }
    is_ali = ConfigUtil.getBool(p, "is_ali");
  }
  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  public void close() {
  }

  // this is invoked when an error happens in case connection needs to be
  // cleaned up, reset, reopened, whatever
  public void clearErrors(int threadID) {}

  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception {
    System.err.println("reset node store is not implemented");
    // System.exit(1);
  }

  /** Construct client for accessing HelloWorld server using the existing channel. */
  // LinkStoreCCGraph(ManagedChannel channel) {
  //   this.channel = channel;
  //   blockingStub = SimpleGraphServerGrpc.newBlockingStub(channel);
  // }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
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

  public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.id1)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.id2)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.link_type)));
    rqst.addParamList(ByteString.copyFrom(a.data));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.visibility)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.version)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.time)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_upsert_link"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (rply.getCode().equals(Code.kAbort)) {
      // id2 not exist
      return false;
    }
    if (!rply.getCode().equals(Code.kOk)) {
      throw new Exception(String.format("Insert link failed: (%d, %d)", a.id1, a.id2) + rply.getTable(0).getOneRow(0).toStringUtf8());
    }

    int inserted = Integer.valueOf(rply.getTable(0).getOneRow(0).toStringUtf8()).intValue();
    return inserted == 1;
  }
  public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.id1)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.id2)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.link_type)));
    rqst.addParamList(ByteString.copyFrom(a.data));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.visibility)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.version)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.time)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_upsert_link"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (rply.getCode().equals(Code.kAbort)) {
      // id2 not exist
      return false;
    }
    if (!rply.getCode().equals(Code.kOk)) {
      throw new Exception(String.format("Update link failed: (%d, %d)", a.id1, a.id2) + rply.getTable(0).getOneRow(0).toStringUtf8());
    }

    int inserted = Integer.valueOf(rply.getTable(0).getOneRow(0).toStringUtf8()).intValue();
    return inserted == 0;
  }
  public boolean deleteLink(String dbid, long id1, long link_type,
      long id2, boolean noinverse, boolean expunge) throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(id1)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(id2)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(link_type)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_delete_link"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (rply.getCode().equals(Code.kOk)) {
      return true;
    } else if (rply.getCode().equals(Code.kAbort)) {
      return false;
    } else {
      throw new Exception(String.format("delete link failed: (%d, %d)", id1, id2));
    }
  }
  public Link getLink(String dbid, long id1, long link_type, long id2) 
      throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(id1)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(id2)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(link_type)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_get_link"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (!rply.getCode().equals(Code.kOk)) {
      throw new Exception(String.format("get link failed: (%d, %d)", id1, id2));
    }

    if (rply.getTableCount() == 0) return null;

    Link ret = new Link();
    ret.id1 = id1;
    ret.link_type = link_type;
    ret.id2 = id2;
    ret.data = rply.getTable(0).getOneRow(0).toByteArray();
    ret.visibility = Byte.valueOf(rply.getTable(0).getOneRow(1).toStringUtf8()).byteValue();
    ret.version = Integer.valueOf(rply.getTable(0).getOneRow(2).toStringUtf8()).intValue();
    ret.time = Long.valueOf(rply.getTable(0).getOneRow(3).toStringUtf8()).longValue();

    return ret;
  }
  @Override
  public Link[] multigetLinks(String dbid, long id1, long link_type,
      long id2s[]) 
      throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(id1)));
    String id2_string = "";
    if (id2s.length > 0) id2_string = String.valueOf(id2s[0]);
    for (int i = 1; i < id2s.length; i++) {
      id2_string = id2_string + "," + String.valueOf(id2s[i]);
    }
    rqst.addParamList(ByteString.copyFromUtf8(id2_string));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(link_type)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_multi_get_link"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (!rply.getCode().equals(Code.kOk)) {
      throw new Exception(String.format("multi get link failed: (%d)", id1));
    }

    if (rply.getTableCount() == 0) return new Link[0];
    Link[] ret = new Link[rply.getTableCount()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = new Link();
      ret[i].id1 = id1;
      ret[i].link_type = link_type; 
      ret[i].id2 = Long.valueOf(rply.getTable(i).getOneRow(0).toStringUtf8()).longValue();
      ret[i].data = rply.getTable(i).getOneRow(1).toByteArray();
      ret[i].visibility = Byte.valueOf(rply.getTable(i).getOneRow(2).toStringUtf8()).byteValue();
      ret[i].version = Integer.valueOf(rply.getTable(i).getOneRow(3).toStringUtf8()).intValue();
      ret[i].time = Long.valueOf(rply.getTable(i).getOneRow(4).toStringUtf8()).longValue();
    }
    return ret;
  }
  public Link[] getLinkList(String dbid, long id1, long link_type) 
      throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }
  public Link[] getLinkList(String dbid, long id1, long link_type,
      long minTimestamp, long maxTimestamp, int offset, int limit) 
      throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(id1)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(link_type)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(minTimestamp)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(maxTimestamp)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(offset)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(limit)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_get_link_list"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (!rply.getCode().equals(Code.kOk)) {
      throw new Exception(String.format("get link list failed: (%d)", id1));
    }

    if (rply.getTableCount() == 0) return new Link[0];
    Link[] ret = new Link[rply.getTableCount()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = new Link();
      ret[i].id1 = id1;
      ret[i].link_type = link_type; 
      ret[i].id2 = Long.valueOf(rply.getTable(i).getOneRow(0).toStringUtf8()).longValue();
      ret[i].data = rply.getTable(i).getOneRow(1).toByteArray();
      ret[i].visibility = Byte.valueOf(rply.getTable(i).getOneRow(2).toStringUtf8()).byteValue();
      ret[i].version = Integer.valueOf(rply.getTable(i).getOneRow(3).toStringUtf8()).intValue();
      ret[i].time = Long.valueOf(rply.getTable(i).getOneRow(4).toStringUtf8()).longValue();
    }
    return ret;
  }
  public long countLinks(String dbid, long id1, long link_type) 
      throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(id1)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(link_type)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_count_link"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (!rply.getCode().equals(Code.kOk)) {
      throw new Exception(String.format("count link failed: (%d)", id1));
    }

    long link_count = Long.valueOf(rply.getTable(0).getOneRow(0).toStringUtf8()).longValue();
    return link_count;
  }

  public long addNode(String dbid, Node node) throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();
    long id = _nodeid.getAndIncrement();
    node.id = id;

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(node.id)));
    rqst.addParamList(ByteString.copyFrom(node.data));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(node.version)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(node.time)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_insert_node"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (!rply.getCode().equals(Code.kOk)) {
      // blockingStub.abort(txnidmsg);
      throw new Exception(String.format("Insert node failed: (%d, %d)", node.id, node.type));
    }

    return id;
  }
  
  public boolean updateNode(String dbid, Node node) throws Exception {
    if (is_ali) return aliUpdateNode(dbid, node);
    else return linkbenchUpdateNode(dbid, node);
  }

  public boolean linkbenchUpdateNode(String dbid, Node node) throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(node.id)));
    rqst.addParamList(ByteString.copyFrom(node.data));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(node.version)));
    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(node.time)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_update_node"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (!rply.getCode().equals(Code.kOk)) {
      // blockingStub.abort(txnidmsg);
      throw new Exception(String.format("Update node failed: (%d, %d)", node.id, node.type) + rply.getTable(0).getOneRow(0).toStringUtf8());
    }
    long node_count = Long.valueOf(rply.getTable(0).getOneRow(0).toStringUtf8()).longValue();

    return node_count > 0;
  }

  public boolean aliUpdateNode(String dbid, Node node) throws Exception {
    long start_time = System.nanoTime();
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    if (ali_given_path) {
      rqst.addParamList(ByteString.copyFromUtf8(AliPath.pathstring.get((int)(node.id))));
      rqst.setTxnName(ByteString.copyFromUtf8("ali_login_given_path"));
    } else {
      rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(node.id)));
      rqst.setTxnName(ByteString.copyFromUtf8("ali_login"));
    }
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    // return true;
    if (!rply.getCode().equals(Code.kOk)) {
      // blockingStub.abort(txnidmsg);
      throw new Exception(String.format("Update node failed: (%d, %d)", node.id, node.type));
    }
    long node_count = Long.valueOf(rply.getTable(0).getOneRow(0).toStringUtf8()).longValue();
    long end_time = System.nanoTime();
    _measurements.measure("count-time", node_count, (end_time - start_time)/1000);
    _measurements.measure("count-cctime", node_count, rply.getMeasure(1));
    _measurements.measure("count-txntime", node_count, rply.getMeasure(2));
    _measurements.measure("count-usedlock", node_count, rply.getMeasure(3)*1000);
    _measurements.measure("count-blockedlock", node_count, rply.getMeasure(5)*1000);

    _measurements.measure("lock_block_time", rply.getMeasure(0));
    _measurements.measure("cc_time", rply.getMeasure(1));
    _measurements.measure("txn_time", rply.getMeasure(2));
    _measurements.measure("used_locks", rply.getMeasure(3)*1000);
    _measurements.measure("reused_locks", rply.getMeasure(4)*1000);
    _measurements.measure("blocked_locks", rply.getMeasure(5)*1000);
    _measurements.measure("retries", rply.getMeasure(6)*1000);
    for (int i = 7; i < rply.getMeasureCount(); i++) {
      _measurements.measure("step["+(i-6)+"]", rply.getMeasure(i));
    }
    _measurements.measure("node_count", node_count*1000);
    return rply.getCode().equals(Code.kOk);
  }

  public boolean deleteNode(String dbid, int type, long id) throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(id)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_delete_node"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (rply.getCode().equals(Code.kAbort)) {
      return false;
    }
    if (!rply.getCode().equals(Code.kOk)) {
      // blockingStub.abort(txnidmsg);
      throw new Exception(String.format("Delete node failed: (%d, %d)", id, type));
    }
    return true;
  }
  public Node getNode(String dbid, int type, long id) throws Exception {
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(id)));

    rqst.setTxnName(ByteString.copyFromUtf8("lb_get_node"));
    rqst.setRetry(true);

    Results rply = blockingStub.runTxn(rqst.build());
    if (!rply.getCode().equals(Code.kOk)) {
      throw new Exception(String.format("get node failed: (%d)", id));
    }

    if (rply.getTableCount() == 0) return null;

    byte data[] = rply.getTable(0).getOneRow(0).toByteArray();
    long version = Long.valueOf(rply.getTable(0).getOneRow(1).toStringUtf8()).longValue();
    int time = Integer.valueOf(rply.getTable(0).getOneRow(2).toStringUtf8()).intValue();

    Node ret = new Node(
      id,
      type,
      version,
      time,
      data
    );

    return ret;
  }
}
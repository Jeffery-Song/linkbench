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
    String host = p.getProperty("host", "127.0.0.1");
    int port = Integer.parseInt(p.getProperty("port", "55555"));
    synchronized(__channel_sync) {
      if (channel == null) 
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
      this.blockingStub = CCGraphServerGrpc.newBlockingStub(channel);
    }
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
    // assert(noinverse == true);
    // com.simplegraph.simplegraph.Link.Builder sglinkbuilder = sgLinkBuilder(a);
    // CallParam.Builder rqst = CallParam.newBuilder();

    // rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.id1)));
    // rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.id2)));
    // rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.link_type)));
    // rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.visibility == LinkStore.VISIBILITY_HIDDEN)));
    // rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.version)));
    // rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(a.time)));
    // rqst.addParamList(ByteString.copyFromUtf8(a.data));

    // rqst.setTxnName(ByteString.copyFromUtf8("linkbench_add_link"));

    // Results rply = blockingStub.runTxn(rqst.build());
    // if (!rply.getCode().equals(Code.kOk)) {
    //   // blockingStub.abort(txnidmsg);
    //   throw new ConflictException(String.format("Add link failed: (%d, %d, %d)", a.id1,
    //                       a.link_type, a.id2));
    // }
    // fixme: 
    // return c.equals(Code.kOk);
    throw new Exception("Unimplemented");
  }
  public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
    throw new Exception("Unimplemented");
  }
  public boolean deleteLink(String dbid, long id1, long link_type,
      long id2, boolean noinverse, boolean expunge) throws Exception {
    throw new Exception("Unimplemented");
  }
  public Link getLink(String dbid, long id1, long link_type, long id2) 
      throws Exception {
    throw new Exception("Unimplemented");
  }
  @Override
  public Link[] multigetLinks(String dbid, long id1, long link_type,
      long id2s[]) 
      throws Exception {
    throw new Exception("Unimplemented");
  }
  public Link[] getLinkList(String dbid, long id1, long link_type) 
      throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }
  public Link[] getLinkList(String dbid, long id1, long link_type,
      long minTimestamp, long maxTimestamp, int offset, int limit) 
      throws Exception {
    checkDBID(dbid);
    throw new Exception("Unimplemented");
  }
  public long countLinks(String dbid, long id1, long link_type) 
      throws Exception {
    checkDBID(dbid);
    throw new Exception("Unimplemented");
  }

  public long addNode(String dbid, Node node) throws Exception {
    checkDBID(dbid);
    throw new Exception("Unimplemented");
  }
  
  public boolean updateNode(String dbid, Node node) throws Exception {
    long start_time = System.nanoTime();
    checkDBID(dbid);

    CallParam.Builder rqst = CallParam.newBuilder();

    rqst.addParamList(ByteString.copyFromUtf8(String.valueOf(node.id)));

    rqst.setTxnName(ByteString.copyFromUtf8("ali_login"));
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
    throw new Exception("Unimplemented");
  }
  public Node getNode(String dbid, int type, long id) throws Exception {
    checkDBID(dbid);
    throw new Exception("Unimplemented");
  }
}
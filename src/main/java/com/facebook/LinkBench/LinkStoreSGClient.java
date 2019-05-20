package com.facebook.LinkBench;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
// import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Properties;
import java.io.IOException;

import com.facebook.LinkBench.GraphStore;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;

import com.google.protobuf.ByteString;

import com.simplegraph.simplegraph.SimpleGraphServerGrpc;
import com.simplegraph.simplegraph.StartParam;
import com.simplegraph.simplegraph.TxnID;
import com.simplegraph.simplegraph.TxnIDParam;
import com.simplegraph.simplegraph.Code;
// import com.simplegraph.simplegraph.CodeRet;
import com.simplegraph.simplegraph.CountLinkRet;
import com.simplegraph.simplegraph.CreateLinkParam;
import com.simplegraph.simplegraph.CreateNodeParam;
import com.simplegraph.simplegraph.UpdateLinkParam;
import com.simplegraph.simplegraph.UpdateNode2Param;
import com.simplegraph.simplegraph.DeleteLinkParam;
import com.simplegraph.simplegraph.DeleteNode2Param;
import com.simplegraph.simplegraph.GetLinkList2Param;
import com.simplegraph.simplegraph.GetLinkListRet;
import com.simplegraph.simplegraph.GetLinkParam;
import com.simplegraph.simplegraph.GetLinkRet;
import com.simplegraph.simplegraph.GetNodeParam;
import com.simplegraph.simplegraph.GetNodeRet;
import com.simplegraph.simplegraph.MultiGetLinkParam;
import com.simplegraph.simplegraph.RowKey;

/**
 * A simple client that requests a greeting from the {@link HelloWorldServer}.
 */
public class LinkStoreSGClient extends GraphStore {
  private static final Logger logger = Logger.getLogger(LinkStoreSGClient.class.getName());

  private static ManagedChannel channel = null;
  private static String __channel_sync = "";
  private SimpleGraphServerGrpc.SimpleGraphServerBlockingStub blockingStub;
  private long txnid;

  private static boolean _dbidready = false;
  private static String _dbid = "";
  private static String __sync = "";
  private static AtomicLong _nodeid = new AtomicLong(1);
  
  /** Construct client connecting to HelloWorld server at {@code host:port}. */
  // public LinkStoreSGClient(String host, int port) {
  //   this(ManagedChannelBuilder.forAddress(host, port)
  //       // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
  //       // needing certificates.
  //       .usePlaintext()
  //       .build());
  // }

  public void initialize(Properties p,
  Phase currentPhase, int threadId) throws IOException, Exception {
    String host = p.getProperty("host", "0.0.0.0");
    int port = Integer.parseInt(p.getProperty("port", "50051"));
    synchronized(__channel_sync) {
      if (channel == null) 
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
      this.blockingStub = SimpleGraphServerGrpc.newBlockingStub(channel);
    }
    StartParam sp = StartParam.getDefaultInstance();
    txnid = blockingStub.start(sp).getTxnid();
  }
  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  public void close() {
    TxnIDParam.Builder rqst = TxnIDParam.newBuilder();
    rqst.setReuse(false);
    rqst.setTxnid(txnid);
    blockingStub.abort(rqst.build());
    // channel.shutdown();
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
  // LinkStoreSGClient(ManagedChannel channel) {
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

  // private com.simplegraph.simplegraph.Link sgLink(Link in) {
  //   if (in == null) return null;
  //   com.simplegraph.simplegraph.Link.Builder lb = com.simplegraph.simplegraph.Link.newBuilder();
  //   // com.simplegraph.simplegraph.LinkOrBuilder lb;
  //   lb.setId1(in.id1);
  //   lb.setId2(in.id2);
  //   lb.setType(in.link_type);
  //   lb.setHidden(in.visibility == LinkStore.VISIBILITY_HIDDEN);
  //   lb.setVersion(in.version);
  //   lb.setTs(in.time);
  //   ByteString bs = ByteString.copyFrom(in.data);
  //   lb.setDataBytes(bs);
  //   return lb.build();
  // }
  private com.simplegraph.simplegraph.Link.Builder sgLinkBuilder(Link in) {
    if (in == null) return null;
    com.simplegraph.simplegraph.Link.Builder lb = com.simplegraph.simplegraph.Link.newBuilder();
    // com.simplegraph.simplegraph.LinkOrBuilder lb;
    lb.setId1(in.id1);
    lb.setId2(in.id2);
    lb.setType(in.link_type);
    lb.setHidden(in.visibility == LinkStore.VISIBILITY_HIDDEN);
    lb.setVersion(in.version);
    lb.setTs(in.time);
    ByteString bs = ByteString.copyFrom(in.data);
    lb.setData(bs);
    return lb;
  }

  private Link lsLink(com.simplegraph.simplegraph.Link in) {
    if (in == null) return null;
    Link out = new Link();
    out.id1 = in.getId1();
    out.id2 = in.getId2();
    out.link_type = in.getType();
    out.visibility = in.getHidden() ? LinkStore.VISIBILITY_HIDDEN : 
                                      LinkStore.VISIBILITY_DEFAULT;
    out.version = in.getVersion();
    out.time = in.getTs();
    out.data = in.getData().toByteArray();
    return out;
  }

  public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
    checkDBID(dbid);
    assert(noinverse == true);
    com.simplegraph.simplegraph.Link.Builder sglinkbuilder = sgLinkBuilder(a);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    CreateLinkParam.Builder rqst = CreateLinkParam.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setLink(sglinkbuilder);
    rqst.setUpdateIfExist(true);
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    Code c = blockingStub.createLink(rqst.build()).getCode();
    if (!c.equals(Code.kOk) && !c.equals(Code.kCoveredCreate)) {
      // blockingStub.abort(txnidmsg);
      throw new ConflictException(String.format("Add link failed: (%d, %d, %d)", a.id1,
                          a.link_type, a.id2));
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   throw new ConflictException(String.format("Add link Conflict: (%d, %d, %d)", a.id1,
    //                       a.link_type, a.id2));
    // }
    return c.equals(Code.kOk);
  }
  public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
    checkDBID(dbid);
    assert(noinverse == true);
    com.simplegraph.simplegraph.Link.Builder sglinkbuilder = sgLinkBuilder(a);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    UpdateLinkParam.Builder rqst = UpdateLinkParam.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setLink(sglinkbuilder);
    rqst.setCreateIfMiss(true);
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    Code c = blockingStub.updateLink(rqst.build()).getCode();
    if (!c.equals(Code.kOk) && !c.equals(Code.kNewedUpdate)) {
      // blockingStub.abort(txnidmsg);
      throw new Exception(String.format("Update link failed: (%d, %d, %d)", a.id1,
                          a.link_type, a.id2));
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   throw new ConflictException(String.format("Update link Conflict: (%d, %d, %d)", a.id1,
    //                       a.link_type, a.id2));
    // }
    return c.equals(Code.kOk);
  }
  public boolean deleteLink(String dbid, long id1, long link_type,
      long id2, boolean noinverse, boolean expunge) throws Exception {
    checkDBID(dbid);
    assert(noinverse == true);
    // com.simplegraph.simplegraph.Link.Builder sglinkbuilder = sgLinkBuilder(a);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    DeleteLinkParam.Builder rqst = DeleteLinkParam.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setId1(id1);
    rqst.setId2(id2);
    rqst.setType(link_type);
    rqst.setHide(expunge == false);
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    Code c = blockingStub.deleteLink(rqst.build()).getCode();
    if (!c.equals(Code.kOk) && !c.equals(Code.kNotExist)) {
      // blockingStub.abort(txnidmsg);
      throw new ConflictException(String.format("Delete link failed: (%d, %d, %d)", id1,
                          link_type, id2));
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   throw new ConflictException(String.format("Delete link conflict: (%d, %d, %d)", id1,
    //                       link_type, id2));
    // }
    return c.equals(Code.kOk);
  }
  public Link getLink(String dbid, long id1, long link_type, long id2) 
      throws Exception {
    checkDBID(dbid);
    // com.simplegraph.simplegraph.Link.Builder sglinkbuilder = sgLinkBuilder(a);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    GetLinkParam.Builder rqst = GetLinkParam.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setId1(id1);
    rqst.setId2(id2);
    rqst.setType(link_type);
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    GetLinkRet lr = blockingStub.getLink(rqst.build());
    Code c = lr.getCode();
    if (!c.equals(Code.kOk) && !c.equals(Code.kNotExist)) {
      // blockingStub.abort(txnidmsg);
      throw new ConflictException(String.format("Get link failed: (%d, %d, %d)", id1,
                          link_type, id2));
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   throw new ConflictException(String.format("Get link conflict: (%d, %d, %d)", id1,
    //                       link_type, id2));
    // }
    if (c.equals(Code.kNotExist)) return null;
    return lsLink(lr.getLink());
  }
  @Override
  public Link[] multigetLinks(String dbid, long id1, long link_type,
      long id2s[]) 
      throws Exception {
    checkDBID(dbid);
    // com.simplegraph.simplegraph.Link.Builder sglinkbuilder = sgLinkBuilder(a);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    MultiGetLinkParam.Builder rqst = MultiGetLinkParam.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setId1(id1);
    rqst.setType(link_type);
    for (long id2 : id2s) {
      rqst.addId2S(id2);
    }
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    GetLinkListRet lr = blockingStub.multiGetLink(rqst.build());
    Code c = lr.getCode();
    if (!c.equals(Code.kOk)) {
      // blockingStub.abort(txnidmsg);
      // throw new Exception(String.format(
      //     "Get link list failed: (id1=%d, lt=%d, code=%d)", id1,
      //     link_type, c.getNumber()));
      return null;
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   // throw new ConflictException(String.format(
    //   //     "Get link list conflict: (id1=%d, lt=%d, code=%d)", id1,
    //   //     link_type, c.getNumber()));
    //   return null;
    // }
    if (lr.getListsCount() == 0) return new Link[0];
    Link[] lslinkarray = new Link[lr.getListsCount()];
    for (int i = 0; i < lslinkarray.length; i++) {
      lslinkarray[i] = lsLink(lr.getLists(i));
    }
    return lslinkarray;
  }
  public Link[] getLinkList(String dbid, long id1, long link_type) 
      throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
    // checkDBID(dbid);
    // // com.simplegraph.simplegraph.Link.Builder sglinkbuilder = sgLinkBuilder(a);
    // // StartParam sp = StartParam.getDefaultInstance();
    // // TxnID txnidmsg = blockingStub.start(sp);
    // RowKey.Builder rqst = RowKey.newBuilder();
    // rqst.setTxnid(txnid);
    // rqst.setId1(id1);
    // rqst.setType(link_type);
    // rqst.setAutocommit(true);
    // rqst.setReuse(true);

    // GetLinkListRet lr = blockingStub.getLinkList(rqst.build());
    // Code c = lr.getCode();
    // if (!c.equals(Code.kOk)) {
    //   // blockingStub.abort(txnidmsg);
    //   // throw new Exception(String.format(
    //   //     "Get link list failed: (id1=%d, lt=%d, code=%d)", id1,
    //   //     link_type, c.getNumber()));
    //   return null;
    // }
    // // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    // //   // throw new ConflictException(String.format(
    // //   //     "Get link list conflict: (id1=%d, lt=%d, code=%d)", id1,
    // //   //     link_type, c.getNumber()));
    // //   return null;
    // // }
    // if (lr.getListsCount() == 0) return new Link[0];
    // Link[] lslinkarray = new Link[lr.getListsCount()];
    // for (int i = 0; i < lslinkarray.length; i++) {
    //   lslinkarray[i] = lsLink(lr.getLists(i));
    // }
    // return lslinkarray;
  }
  public Link[] getLinkList(String dbid, long id1, long link_type,
      long minTimestamp, long maxTimestamp, int offset, int limit) 
      throws Exception {
    checkDBID(dbid);
    // com.simplegraph.simplegraph.Link.Builder sglinkbuilder = sgLinkBuilder(a);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    GetLinkList2Param.Builder rqst = GetLinkList2Param.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setId1(id1);
    rqst.setType(link_type);
    rqst.setMaxts(maxTimestamp);
    rqst.setMints(minTimestamp);
    rqst.setAutocommit(true);
    rqst.setReuse(true);
    rqst.setOffset(offset);
    rqst.setLimit(limit);

    GetLinkListRet lr = blockingStub.getLinkList2(rqst.build());
    Code c = lr.getCode();
    if (!c.equals(Code.kOk)) {
      // blockingStub.abort(txnidmsg);
      // throw new Exception(String.format(
      //     "Get link list failed: (id1=%d, lt=%d, code=%d)", id1,
      //     link_type, c.getNumber()));
      return null;
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   // throw new ConflictException(String.format(
    //   //     "Get link list conflict: (id1=%d, lt=%d, code=%d)", id1,
    //   //     link_type, c.getNumber()));
    //   return null;
    // }
    // int expectedSize = (int)lr.getListsCount() - offset;
    // if (expectedSize < 0) expectedSize = 0;
    // if (expectedSize > limit) expectedSize = limit;
    if (lr.getListsCount() == 0) {
      return new Link[0];
    }
    Link[] lslinkarray = new Link[lr.getListsCount()];
    for (int i = 0; i < lslinkarray.length; i++) {
      lslinkarray[i] = lsLink(lr.getLists(i));
    }
    return lslinkarray;
  }
  public long countLinks(String dbid, long id1, long link_type) 
      throws Exception {
    checkDBID(dbid);
    // com.simplegraph.simplegraph.Link.Builder sglinkbuilder = sgLinkBuilder(a);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    RowKey.Builder rqst = RowKey.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setId1(id1);
    rqst.setType(link_type);
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    CountLinkRet clr = blockingStub.countLink(rqst.build());
    Code c = clr.getCode();
    if (!c.equals(Code.kOk)) {
      // blockingStub.abort(txnidmsg);
      throw new ConflictException(String.format("Count link failed: (%d, %d)", id1,
                          link_type));
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   throw new ConflictException(String.format("Count link conflict: (%d, %d)", id1,
    //                       link_type));
    // }
    return clr.getCount();
  }

  
  // private com.simplegraph.simplegraph.Node sgNode(Node in) {
  //   if (in == null) return null;
  //   com.simplegraph.simplegraph.Node.Builder nb = com.simplegraph.simplegraph.Node.newBuilder();
  //   // com.simplegraph.simplegraph.LinkOrBuilder lb;
  //   nb.setId(in.id);
  //   nb.setType(in.type);
  //   nb.setVersion(in.version);
  //   nb.setTs(in.time);
  //   ByteString bs = ByteString.copyFrom(in.data);
  //   nb.setDataBytes(bs);
  //   return nb.build();
  // }
  private com.simplegraph.simplegraph.Node.Builder sgNodeBuilder(Node in) {
    if (in == null) return null;
    com.simplegraph.simplegraph.Node.Builder nb = com.simplegraph.simplegraph.Node.newBuilder();
    // com.simplegraph.simplegraph.LinkOrBuilder lb;
    nb.setId(in.id);
    nb.setType(in.type);
    nb.setVersion(in.version);
    nb.setTs(in.time);
    ByteString bs = ByteString.copyFrom(in.data);
    nb.setData(bs);
    return nb;
  }

  private Node lsNode(com.simplegraph.simplegraph.Node in) {
    if (in == null) return null;
    Node out = new Node(
      in.getId(),
      in.getType(),
      in.getVersion(),
      in.getTs(),
      in.getData().toByteArray()
    );
    return out;
  }
  public long addNode(String dbid, Node node) throws Exception {
    checkDBID(dbid);
    long allocatedID = _nodeid.getAndIncrement();
    com.simplegraph.simplegraph.Node.Builder sgnodebuilder = sgNodeBuilder(node);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    CreateNodeParam.Builder rqst = CreateNodeParam.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setNode(sgnodebuilder);
    rqst.setId(allocatedID);
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    Code c = blockingStub.createNode(rqst.build()).getCode();
    if (!c.equals(Code.kOk) && !c.equals(Code.kAlreadyExist)) {
      // blockingStub.abort(txnidmsg);
      throw new ConflictException(String.format("Add node failed: (%d)", allocatedID));
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   throw new ConflictException(String.format("Add node conflict: (%d)", allocatedID));
    // }
    return allocatedID;
  }
  
  public boolean updateNode(String dbid, Node node) throws Exception {
    checkDBID(dbid);
    com.simplegraph.simplegraph.Node.Builder sgnodebuilder = sgNodeBuilder(node);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    UpdateNode2Param.Builder rqst = UpdateNode2Param.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setNode(sgnodebuilder);
    rqst.setId(node.id);
    rqst.setType(node.type);
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    Code c = blockingStub.updateNode2(rqst.build()).getCode();
    if (!c.equals(Code.kOk) && !c.equals(Code.kNotExist) && !c.equals(Code.kMissMatchNodeType)) {
      // blockingStub.abort(txnidmsg);
      throw new ConflictException(String.format("Update node failed: (%d, %d)", node.id, node.type));
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   throw new ConflictException(String.format("Update node conflict: (%d, %d)", node.id, node.type));
    // }
    return c.equals(Code.kOk);
  }
  public boolean deleteNode(String dbid, int type, long id) throws Exception {
    checkDBID(dbid);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    DeleteNode2Param.Builder rqst = DeleteNode2Param.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setId(id);
    rqst.setType(type);
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    Code c = blockingStub.deleteNode2(rqst.build()).getCode();
    if (!c.equals(Code.kOk) && !c.equals(Code.kNotExist) && !c.equals(Code.kMissMatchNodeType)) {
      // blockingStub.abort(txnidmsg);
      throw new ConflictException(String.format("Delete node failed: (%d, %d)", id, type));
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   throw new ConflictException(String.format("Delete node failed: (%d, %d)", id, type));
    // }
    return c.equals(Code.kOk);
  }
  public Node getNode(String dbid, int type, long id) throws Exception {
    checkDBID(dbid);
    // StartParam sp = StartParam.getDefaultInstance();
    // TxnID txnidmsg = blockingStub.start(sp);
    GetNodeParam.Builder rqst = GetNodeParam.newBuilder();
    rqst.setTxnid(txnid);
    rqst.setId(id);
    rqst.setAutocommit(true);
    rqst.setReuse(true);

    GetNodeRet gnr = blockingStub.getNode(rqst.build());
    Code c = gnr.getCode();
    if (!c.equals(Code.kOk) && !c.equals(Code.kNotExist)) {
      // blockingStub.abort(txnidmsg);
      throw new ConflictException(String.format("Get node failed: (%d)", id));
    }
    // if (blockingStub.commit(txnidmsg).getCode().equals(Code.kOk) == false) {
    //   throw new ConflictException(String.format("Get node conflict: (%d)", id));
    // }
    if (c.equals(Code.kNotExist)) return null;
    if (c.equals(Code.kOk) && gnr.getNode().getType() != type) {
      return null;
    }
    return lsNode(gnr.getNode());
  }
}
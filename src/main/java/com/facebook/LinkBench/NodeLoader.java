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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

// import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.alimeta.UserMapping;
import com.facebook.LinkBench.alimeta.UserToDev;
import com.facebook.LinkBench.alimeta.UserToIP;
import com.facebook.LinkBench.distributions.LogNormalDistribution;
import com.facebook.LinkBench.generators.DataGenerator;
import com.facebook.LinkBench.stats.LatencyStats;
import com.facebook.LinkBench.stats.SampledStats;
import com.facebook.LinkBench.util.ClassLoadUtil;

/**
 * Load class for generating node data
 *
 * This is separate from link loading because we can't have multiple parallel
 * loaders loading nodes, as the order of IDs being assigned would be messed up
 * @author tarmstrong
 */
public class NodeLoader implements Runnable {
  private static final long REPORT_INTERVAL = 25000;
  private final Properties props;
  private final Logger logger;
  private final GraphSerializerCSV csvGraph;
  private final Random rng;
  private final String dbid;

  // Data generation settings
  private final DataGenerator nodeDataGen;
  private final LogNormalDistribution nodeDataLength;

  // private final Level debuglevel;
  private final int loaderId;
  private final SampledStats stats;
  private final LatencyStats latencyStats;

  private long startTime_ms;

  private long nodesLoaded = 0;
  private long totalNodes = 0;

  /** Next node count to report on */
  private long nextReport = 0;


  /** Last time stat update displayed */
  private long lastDisplayTime_ms;

  /** How often to display stat updates */
  private final long displayFreq_ms;

  private boolean is_ali;

  private NodeGenerator nodeGenerator;

  public NodeLoader(Properties props, Logger logger,
      NodeStore nodeStore, Random rng,
      LatencyStats latencyStats, PrintStream csvStreamOut, int loaderId) {
    super();
    this.props = props;
    this.logger = logger;
    // FIXME: should the csvgraph be shared?
    this.csvGraph = new GraphSerializerCSV();
    // this.nodeStore = nodeStore;
    this.rng = rng;
    this.latencyStats = latencyStats;
    this.loaderId = loaderId;

    double medianDataLength = ConfigUtil.getDouble(props, Config.NODE_DATASIZE);
    nodeDataLength = new LogNormalDistribution();
    nodeDataLength.init(0, NodeStore.MAX_NODE_DATA, medianDataLength,
                                          Config.NODE_DATASIZE_SIGMA);

    try {
      nodeDataGen = ClassLoadUtil.newInstance(
          ConfigUtil.getPropertyRequired(props, Config.NODE_ADD_DATAGEN),
          DataGenerator.class);
      nodeDataGen.init(props, Config.NODE_ADD_DATAGEN_PREFIX);
    } catch (ClassNotFoundException ex) {
      logger.error(ex);
      throw new LinkBenchConfigError("Error loading data generator class: "
            + ex.getMessage());
    }

    // debuglevel = ConfigUtil.getDebugLevel(props);
    dbid = ConfigUtil.getPropertyRequired(props, Config.DBID);


    displayFreq_ms = ConfigUtil.getLong(props, Config.DISPLAY_FREQ) * 1000;
    int maxsamples = ConfigUtil.getInt(props, Config.MAX_STAT_SAMPLES);
    this.stats = new SampledStats(loaderId, maxsamples, csvStreamOut);
    is_ali = ConfigUtil.getBool(props, "is_ali");
    if (is_ali) {
      nodeGenerator = new AliNodeGenerator();
    } else {
      nodeGenerator = new LBNodeGenerator();
    }
  }

  private interface NodeGenerator {
    public void doLoad();
  }
  private class LBNodeGenerator implements NodeGenerator {
    /**
     * Create and insert the node into the DB
     * @param rng
     * @param id1
     */
    private void genNode(Random rng, long id1, ArrayList<NodeBase> nodeLoadBuffer,
                            int bulkLoadBatchSize) {
      int dataLength = (int)nodeDataLength.choose(rng);
      Node node = new Node(id1, LinkStore.DEFAULT_NODE_TYPE, 1,
                          (int)(System.currentTimeMillis()/1000),
                          nodeDataGen.fill(rng, new byte[dataLength]));
      nodeLoadBuffer.add(node);
      if (nodeLoadBuffer.size() >= bulkLoadBatchSize) {
        loadNodes(nodeLoadBuffer);
        nodeLoadBuffer.clear();
      }
    }
    public void doLoad() {
      int bulkLoadBatchSize = csvGraph.bulkLoadBatchSize();
      ArrayList<NodeBase> nodeLoadBuffer = new ArrayList<NodeBase>(bulkLoadBatchSize);

      long maxId = ConfigUtil.getLong(props, Config.MAX_ID);
      long startId = ConfigUtil.getLong(props, Config.MIN_ID);
      totalNodes = maxId - startId;
      nextReport = startId + REPORT_INTERVAL;
      startTime_ms = System.currentTimeMillis();
      lastDisplayTime_ms = startTime_ms;
      for (long id = startId; id < maxId; id++) {
        genNode(rng, id, nodeLoadBuffer, bulkLoadBatchSize);

        long now = System.currentTimeMillis();
        if (lastDisplayTime_ms + displayFreq_ms <= now) {
          displayAndResetStats();
        }
        if (id % 100000 == 0 && id != 0) {
          logger.info("Loading of nodes " + id + "/" + totalNodes + " done");
        }
      }
      // Load any remaining data
      loadNodes(nodeLoadBuffer);

      logger.info("Loading of nodes [" + startId + "," + maxId + ") done");
      displayAndResetStats();
      csvGraph.close();
    }
  }

  private class AliNodeGenerator implements NodeGenerator {
      private UserToDev userToDev;
      private UserToIP userToIP;
      private UserMapping devCnt;
      private UserMapping ipCnt;
    private void genUserNode(Random rng, long id1, ArrayList<NodeBase> nodeLoadBuffer,
                            int bulkLoadBatchSize) {
      UserNode user = new UserNode(id1, 0,
                          (System.currentTimeMillis()/1000));
      nodeLoadBuffer.add(user);
      if (nodeLoadBuffer.size() >= bulkLoadBatchSize) {
        loadNodes(nodeLoadBuffer);
        nodeLoadBuffer.clear();
      }
    }
    private void genDeviceNode(int devId, ArrayList<NodeBase> nodeLoadBuffer, int bulkLoadBatchSize) {
      if (devCnt.useCount[devId] == 0) return;
      DeviceNode devNode = new DeviceNode(UserToDev.idToDevMac(devId), 0L);
      nodeLoadBuffer.add(devNode);
      if (nodeLoadBuffer.size() >= bulkLoadBatchSize) {
        loadNodes(nodeLoadBuffer);
        nodeLoadBuffer.clear();
      }
    }
    private void genIPNode(int ipId, ArrayList<NodeBase> nodeLoadBuffer, int bulkLoadBatchSize) {
      if (ipCnt.useCount[ipId] == 0) return;
      IPNode ipNode = new IPNode(UserToIP.idToIpAddr(ipId), 0L);
      nodeLoadBuffer.add(ipNode);
      if (nodeLoadBuffer.size() >= bulkLoadBatchSize) {
        loadNodes(nodeLoadBuffer);
        nodeLoadBuffer.clear();
      }
    }
    public void doLoad() {
      int bulkLoadBatchSize = csvGraph.bulkLoadBatchSize();
      ArrayList<NodeBase> nodeLoadBuffer = new ArrayList<NodeBase>(bulkLoadBatchSize);

      long maxId = ConfigUtil.getLong(props, Config.MAX_ID);
      long startId = ConfigUtil.getLong(props, Config.MIN_ID);

      userToDev = new UserToDev(props);
      userToIP = new UserToIP(props);
      devCnt = UserMapping.getDevInstance();
      devCnt.initialize((int)userToDev.totalCount);
      ipCnt = UserMapping.getIpInstance();
      ipCnt.initialize((int)userToIP.totalCount);

      totalNodes = maxId - startId;
      nextReport = startId + REPORT_INTERVAL;
      startTime_ms = System.currentTimeMillis();
      lastDisplayTime_ms = startTime_ms;
      for (long userId = startId; userId < maxId; userId++) {
        genUserNode(rng, userId, nodeLoadBuffer, bulkLoadBatchSize);
        long[] devIdList = userToDev.mapUserToId(userId);
        for (int i = 0; i < userToDev.metaCountOnLoad(userId); i++) {
          devCnt.useCount[(int)(devIdList[i])] ++;
        }
        long[] ipIdList = userToIP.mapUserToId(userId);
        for (int i = 0; i < userToIP.metaCountOnLoad(userId); i++) {
          ipCnt.useCount[(int)(ipIdList[i])] ++;
        }

        long now = System.currentTimeMillis();
        if (lastDisplayTime_ms + displayFreq_ms <= now) {
          displayAndResetStats();
        }
        if (userId % 100000 == 0 && userId != 0) {
          logger.info("Loading of user nodes " + userId + "/" + totalNodes + " done");
        }
      }
      loadNodes(nodeLoadBuffer);
      logger.info("Loading of user nodes [" + startId + "," + maxId + ") done");

      for (int devId = 1; devId <= userToDev.totalCount; devId++) {
        genDeviceNode(devId, nodeLoadBuffer, bulkLoadBatchSize);
        long now = System.currentTimeMillis();
        if (lastDisplayTime_ms + displayFreq_ms <= now) {
          displayAndResetStats();
        }
        if (devId % 100000 == 0 && devId != 0) {
          logger.info("Loading of device nodes " + devId + "/" + userToDev.totalCount + " done");
        }
      }
      loadNodes(nodeLoadBuffer);
      
      for (int ipId = 1; ipId <= userToIP.totalCount; ipId++) {
        genIPNode(ipId, nodeLoadBuffer, bulkLoadBatchSize);
        long now = System.currentTimeMillis();
        if (lastDisplayTime_ms + displayFreq_ms <= now) {
          displayAndResetStats();
        }
        if (ipId % 100000 == 0 && ipId != 0) {
          logger.info("Loading of ip nodes " + ipId + "/" + userToIP.totalCount + " done");
        }
      }
      loadNodes(nodeLoadBuffer);

      displayAndResetStats();
      csvGraph.close();
    }
  }
  @Override
  public void run() {
    logger.info("Starting loader thread  #" + loaderId + " loading nodes");

    try {
      this.csvGraph.initialize(props, Phase.LOAD, loaderId);
    } catch (Exception e) {
      logger.error("Error while initializing store", e);
      throw new RuntimeException(e);
    }
    nodeGenerator.doLoad();
  }

  private void displayAndResetStats() {
    long now = System.currentTimeMillis();
    stats.displayStats(lastDisplayTime_ms, now,
                       Arrays.asList(LinkBenchOp.LOAD_NODE_BULK));
    stats.resetSamples();
    lastDisplayTime_ms = now;
  }


  private void loadNodes(ArrayList<NodeBase> nodeLoadBuffer) {
    long timestart = System.nanoTime();
    try {
      csvGraph.bulkAddNodes(dbid, nodeLoadBuffer);
      long timetaken = (System.nanoTime() - timestart);
      nodesLoaded += nodeLoadBuffer.size();

      nodeLoadBuffer.clear();

      // convert to microseconds
      stats.addStats(LinkBenchOp.LOAD_NODE_BULK, timetaken/1000, false);
      latencyStats.recordLatency(loaderId,
                    LinkBenchOp.LOAD_NODE_BULK, timetaken/1000);

      if (nodesLoaded >= nextReport) {
        double totalTimeTaken = (System.currentTimeMillis() - startTime_ms) / 1000.0;
        logger.debug(String.format(
            "Loader #%d: %d/%d nodes loaded at %f nodes/sec",
            loaderId, nodesLoaded, totalNodes,
            nodesLoaded / totalTimeTaken));
        nextReport += REPORT_INTERVAL;
      }
    } catch (Throwable e){//Catch exception if any
      long endtime2 = System.nanoTime();
      long timetaken2 = (endtime2 - timestart)/1000;
      logger.error("Error: " + e.getMessage(), e);
      stats.addStats(LinkBenchOp.LOAD_NODE_BULK, timetaken2, true);
      nodeLoadBuffer.clear();
      return;
    }
  }

  public long getNodesLoaded() {
    return nodesLoaded;
  }
}

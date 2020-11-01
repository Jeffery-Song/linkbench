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
package com.facebook.LinkBench.distributions;

import java.util.Properties;
import java.util.Random;

import java.util.concurrent.ThreadLocalRandom;

import com.facebook.LinkBench.Config;
import com.facebook.LinkBench.LinkBenchConfigError;
import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.InvertibleShuffler;
import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.RealDistribution;
import com.facebook.LinkBench.RealDistribution.DistributionType;
import com.facebook.LinkBench.distributions.LinkDistributions.LinkDistribution;

/**
 * Encapsulate logic for choosing id2s for request workload.
 * @author tarmstrong
 *
 */
public class AliID2Chooser extends ID2ChooserBase {

  private ID2Chooser inner_chooser; 
  private long referrer_seed;

  public AliID2Chooser(Properties props, long startid1, long maxid1,
                    int nrequesters, int requesterID) {
    if (ConfigUtil.getInt(props, Config.LINK_TYPE_COUNT, 1) != 1) {
      throw new LinkBenchConfigError("Ali workload should only contains 1 link type in the config file");
    }
    this.inner_chooser = new ID2Chooser(props, startid1, maxid1, nrequesters, requesterID);
    this.referrer_seed = ConfigUtil.getLong(props, Config.REFERRER_SEED, System.currentTimeMillis());
  }

  /**
   * Choose an ids
   * @param rng
   * @param id1
   * @param link_type
   * @param outlink_ix this is the ith link of this type for this id1
   * @return
   */
  public long chooseForLoad(Random rng, long id1, long link_type,
                                                  long outlink_ix) {
    if (link_type == LinkStore.REFERRER_LINK_TYPE) {
      if (id1 == 1 || id1 == 0) {
        System.err.println("id 0 1 can not have referrer");
        System.exit(1);
      }
      if (calcLinkCount(id1, link_type) <= outlink_ix) {
        System.err.println("out link ix exceeds out link count");
        System.exit(1);
      }
      Random r = new Random(id1 * referrer_seed * referrer_seed);
      // generate random id inali_referrer_seed range [1,id1)
      return Math.floorMod(r.nextLong(), id1 - 1) + 1;
    }
    return inner_chooser.chooseForLoad(rng, id1, link_type, outlink_ix);
  }

  /**
   * Choose an id2 for an operation given an id1
   * @param id1
   * @param linkType
   * @param pExisting approximate probability that id should be in
   *        existing range
   * @return
   */
  public long chooseForOp(Random rng, long id1, long linkType,
                                                double pExisting) throws Exception {
    // if type is ref, this must be the insert new node case
    if (linkType == LinkStore.REFERRER_LINK_TYPE) {
      // in insert case, id1 is unknown, so we just randomly generate id2.
      // but how do we determine the range?
      if (id1 == 1 || id1 == 0) throw new Exception("id 0 1 can not have referrer");
      if (calcLinkCount(id1, linkType) == 0) throw new Exception("this new ndoe should not have referrer");
      Random r = new Random(id1 * referrer_seed * referrer_seed);
      // generate random id in range [1,id1)
      return Math.floorMod(r.nextLong(), id1 - 1) + 1;
    }
    return inner_chooser.chooseForOp(rng, id1, LinkStore.DEFAULT_LINK_TYPE, pExisting);
  }

  public long[] chooseMultipleForOp(Random rng, long id1, long linkType,
      int nid2s, double pExisting) throws Exception  {
    throw new Exception("ali chooser not allowed to use multi choose");
  }

  public long[] getLinkTypes() {
    return new long[] {LinkStore.REFERRER_LINK_TYPE, LinkStore.DEFAULT_LINK_TYPE};
  }

  /**
   * Choose a link type.
   * For now just select each type with equal probability.
   */
  public long chooseRandomLinkType(Random rng) {
    return LinkStore.DEFAULT_LINK_TYPE;
  }

  public long calcLinkCount(long id1, long linkType) {
    if (linkType == LinkStore.REFERRER_LINK_TYPE) {
      if (id1 == 1) return 0;
      Random r = new Random(id1 * referrer_seed);
      return (r.nextInt(10) >= 1) ? 1 : 0;
    }
    return inner_chooser.calcLinkCount(id1, linkType);
  }

}

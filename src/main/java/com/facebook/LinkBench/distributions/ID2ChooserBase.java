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

import com.facebook.LinkBench.Config;
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
public abstract class ID2ChooserBase {

  public boolean sameShuffle;
  /**
   * Choose an ids
   * @param rng
   * @param id1
   * @param link_type
   * @param outlink_ix this is the ith link of this type for this id1
   * @return
   */
  public abstract long chooseForLoad(Random rng, long id1, long link_type,
                                                  long outlink_ix);

  /**
   * Choose an id2 for an operation given an id1
   * @param id1
   * @param linkType
   * @param pExisting approximate probability that id should be in
   *        existing range
   * @return
   */
  public abstract long chooseForOp(Random rng, long id1, long linkType,
                                                double pExisting) throws Exception;


  public abstract long[] chooseMultipleForOp(Random rng, long id1, long linkType,
      int nid2s, double pExisting)  throws Exception ;

  public abstract long[] getLinkTypes();

  /**
   * Choose a link type.
   * For now just select each type with equal probability.
   */
  public abstract long chooseRandomLinkType(Random rng);

  public abstract long calcLinkCount(long id1, long linkType);

}

package com.facebook.LinkBench.alimeta;

import java.util.Random;

public abstract class AliUserToMeta {
    public long totalCount;
    private long mapUserToIdSeed;
    private int perUserCount;
    // in load phase, we need to record each device/ip's user count. these are
    // handled in loader, not here.
    
    public AliUserToMeta(long tC, long seed, int pUC) {
        initialize(tC, seed, pUC);
    }
    public AliUserToMeta() {}
    public void initialize(long tC, long seed, int pUC) {
        totalCount = tC;
        mapUserToIdSeed = seed;
        perUserCount = pUC;
        System.err.println(String.format("AliUserToMeta:Using %d as per user count", pUC));
    }
    public long[] mapUserToId(long userId) {
        // TODO: more distribution. currently we only use the simpliest static uniform distribution.
        Random r = new Random(userId * mapUserToIdSeed * mapUserToIdSeed);
        long[] idList = new long[perUserCount];
        for (int i = 0; i < perUserCount; i++) {
            idList[i] = (r.nextLong() % totalCount + totalCount) % totalCount + 1;
        }
        return idList;
    }
    public int metaCountOnLoad(long userId) {
        Random r = new Random((userId * mapUserToIdSeed) ^ mapUserToIdSeed);
        return r.nextInt(perUserCount) + 1;
    }
    /**
     * Maps the meta id to string data, i.e. device id to device mac address
     * id: [1, totalCount]
     */
}

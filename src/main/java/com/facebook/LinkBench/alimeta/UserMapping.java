package com.facebook.LinkBench.alimeta;

// stores the usage count of each device/ip on the load time
public class UserMapping {
    public int[] useCount = null;
    public long[] timestamp = null;
    private UserMapping (){}
    public synchronized void initialize(int totalCount) {
        if (useCount == null) {
            useCount = new int[totalCount + 1];
            timestamp = new long[totalCount + 1];
        }
    }

    private static UserMapping devInst = new UserMapping();
    public static UserMapping getDevInstance() {  
        return devInst;
    }

    private static UserMapping ipInst = new UserMapping();
    public static UserMapping getIpInstance() {  
        return ipInst;
    }
}

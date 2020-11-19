package com.facebook.LinkBench.alimeta;

import java.util.Properties;
import com.facebook.LinkBench.ConfigUtil;

public class UserToIP extends AliUserToMeta {
    public UserToIP() {

    }
    public UserToIP(Properties p) {
        initialize(p);
    }
    public void initialize(Properties p) {
        long totalIPCount = ConfigUtil.getLong(p, "ali.total_ip_count");
        long seed = ConfigUtil.getLong(p, "ali.ip_rand_seed", 0x43215678L);
        int perUserCount = ConfigUtil.getInt(p, "ali.ip_per_user", 4);
        System.err.println(String.format("Using %d as per user count", perUserCount));
        super.initialize(totalIPCount, seed, perUserCount);
    }
    static public byte[] idToString(long id) {
        // TODO: we simply use each byte as the ip address
        // id may vary from [1 to totalCount]
        StringBuilder sb = new StringBuilder();
        byte b = 0;
        b = (byte)((id >> 24) & 0x0ffL);
        sb.append(String.format("%d.", Byte.toUnsignedInt(b)));
        b = (byte)((id >> 16) & 0x0ffL);
        sb.append(String.format("%d.", Byte.toUnsignedInt(b)));
        b = (byte)((id >> 8)  & 0x0ffL);
        sb.append(String.format("%d.", Byte.toUnsignedInt(b)));
        b = (byte)((id >> 0)  & 0x0ffL);
        sb.append(String.format("%d", Byte.toUnsignedInt(b)));
        // TODO Auto-generated method stub
        return sb.toString().getBytes();
    }
    static public byte[] idToIpAddr(long id) {
        return idToString(id);
    }
}

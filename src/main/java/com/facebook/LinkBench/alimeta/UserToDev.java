package com.facebook.LinkBench.alimeta;

import java.util.Properties;
import com.facebook.LinkBench.ConfigUtil;

public class UserToDev extends AliUserToMeta {
    public UserToDev() {

    }
    public UserToDev(Properties p) {
        initialize(p);
    }
    public void initialize(Properties p) {
        long totalDeviceCount = ConfigUtil.getLong(p, "ali.total_device_count");
        long seed = ConfigUtil.getLong(p, "ali.device_rand_seed", 0x12348765L);
        int perUserCount = ConfigUtil.getInt(p, "ali.device_per_user", 3);
        System.err.println(String.format("UserToDev: Using %d as per user count", perUserCount));
        super.initialize(totalDeviceCount, seed, perUserCount);
    }
    public static byte[] idToString(long id) {
        // TODO: we simply use each byte as the mac address
        // id may vary from [1 to totalCount]
        StringBuilder sb = new StringBuilder();
        byte b = 0;
        b = (byte)((id >> 40) & 0x0ffL);
        sb.append(String.format("%02X",  b));
        b = (byte)((id >> 32) & 0x0ffL);
        sb.append(String.format(":%02X", b));
        b = (byte)((id >> 24) & 0x0ffL);
        sb.append(String.format(":%02X", b));
        b = (byte)((id >> 16) & 0x0ffL);
        sb.append(String.format(":%02X", b));
        b = (byte)((id >> 8)  & 0x0ffL);
        sb.append(String.format(":%02X", b));
        b = (byte)((id >> 0)  & 0x0ffL);
        sb.append(String.format(":%02X", b));
        return sb.toString().getBytes();
    }
    public static byte[] idToDevMac(long id) {
        return idToString(id);
    }
}

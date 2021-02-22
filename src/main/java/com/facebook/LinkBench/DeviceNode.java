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

import java.util.Arrays;

import com.facebook.LinkBench.alimeta.UserToDev;

/**
 * Object node in social graph
 * @author tarmstrong
 */
public class DeviceNode extends DevIPNode {
  public int getType() { return 2; }

  public DeviceNode(byte address[], long time) {
    super(address, time);
    this.address = address;
    this.time = time;
  }

  @Override
  protected String embedAddr() {
    return Long.toString(UserToDev.byteMacToEmbed(address));
  }

  public DeviceNode clone() {
    return new DeviceNode(address, time);
  }
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof DeviceNode)) {
      return false;
    }
    DeviceNode o = (DeviceNode) other;
    return Arrays.equals(address, o.address) && time == o.time;
  }

  public String toString() {
    return "DeviceNode(" + "timestamp=" + time + ",address="
                   + Arrays.toString(address) + ")";
  }
}

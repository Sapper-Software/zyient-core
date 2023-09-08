/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.common.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NetUtils {
    public static List<InetAddress> getInetAddresses() throws Exception {
        List<InetAddress> addresses = new ArrayList<>();
        Enumeration e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress i = (InetAddress) ee.nextElement();
                addresses.add(i);
            }
        }
        return addresses;
    }

    public static InetAddress getInetAddress(List<InetAddress> addresses) {
        for (InetAddress address : addresses) {
            if (address instanceof Inet4Address) {
                if (!address.isLoopbackAddress()) {
                    try {
                        if (NetworkInterface.getByInetAddress(address) != null) {
                            return address;
                        }
                    } catch (Exception e) {
                        // do nothing...
                    }
                }
            }
        }
        return null;
    }
}
/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

import lombok.NonNull;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NetUtils {
    public static final String IPV4_REGEX = "^((0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)\\.){3}(0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)$";
    public static final Pattern ipv4Pattern = Pattern.compile(IPV4_REGEX);

    public static boolean isIPV4Address(@NonNull String value) {
        Matcher m = ipv4Pattern.matcher(value);
        return m.matches();
    }

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

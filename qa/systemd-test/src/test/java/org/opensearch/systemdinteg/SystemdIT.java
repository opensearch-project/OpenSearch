/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Licensed to Elasticsearch under one or more contributor
* license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright
* ownership. Elasticsearch licenses this file to you under
* the Apache License, Version 2.0 (the "License"); you may
* not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.systemdinteg;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class SystemdIT {

    private static String opensearchPid;

    @BeforeClass
    public static void setup() throws IOException, InterruptedException {
        opensearchPid = getOpenSearchPid();

        if (opensearchPid.isEmpty()) {
            throw new RuntimeException("Failed to find OpenSearch process ID");
        }
    }

    private static String getOpenSearchPid() throws IOException, InterruptedException {
        String command = "systemctl show --property=MainPID opensearch";
        String output = executeCommand(command, "Failed to get OpenSearch PID");
        return output.replace("MainPID=", "").trim();
    }

    private boolean checkPathExists(String path) throws IOException, InterruptedException {
        String command = String.format("test -e %s && echo true || echo false", path);
        return Boolean.parseBoolean(executeCommand(command, "Failed to check path existence"));
    }

    private boolean checkPathReadable(String path) throws IOException, InterruptedException {
        String command = String.format("su opensearch -s /bin/sh -c 'test -r %s && echo true || echo false'", path);
        return Boolean.parseBoolean(executeCommand(command, "Failed to check read permission"));
    }

    private boolean checkPathWritable(String path) throws IOException, InterruptedException {
        String command = String.format("su opensearch -s /bin/sh -c 'test -w %s && echo true || echo false'", path);
        return Boolean.parseBoolean(executeCommand(command, "Failed to check write permission"));
    }

    private String getPathOwnership(String path) throws IOException, InterruptedException {
        String command = String.format("stat -c '%%U:%%G' %s", path);
        return executeCommand(command, "Failed to get path ownership");
    }

    private static String executeCommand(String command, String errorMessage) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(new String[]{"bash", "-c", command});
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
            if (process.waitFor() != 0) {
                throw new RuntimeException(errorMessage);
            }
            return output.toString().trim();
        }
    }

    @Test
    public void testReadOnlyPaths() throws IOException, InterruptedException {
        String[] readOnlyPaths = {
            "/etc/os-release", "/usr/lib/os-release", "/etc/system-release",
            "/proc/self/mountinfo", "/proc/diskstats",
            "/proc/self/cgroup", "/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpu/-",
            "/sys/fs/cgroup/cpuacct", "/sys/fs/cgroup/cpuacct/-",
            "/sys/fs/cgroup/memory", "/sys/fs/cgroup/memory/-"
        };

        for (String path : readOnlyPaths) {
            if (checkPathExists(path)) {
                assertTrue("Path should be readable: " + path, checkPathReadable(path));
                assertFalse("Path should not be writable: " + path, checkPathWritable(path));
            }
        }
    }

    @Test
    public void testReadWritePaths() throws IOException, InterruptedException {
        String[] readWritePaths = {"/var/log/opensearch", "/var/lib/opensearch"};
        for (String path : readWritePaths) {
            assertTrue("Path should exist: " + path, checkPathExists(path));
            assertTrue("Path should be readable: " + path, checkPathReadable(path));
            assertTrue("Path should be writable: " + path, checkPathWritable(path));
            assertEquals("Path should be owned by opensearch:opensearch", "opensearch:opensearch", getPathOwnership(path));
        }
    }

}

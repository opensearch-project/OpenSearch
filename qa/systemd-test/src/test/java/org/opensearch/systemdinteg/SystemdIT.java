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
        String command = String.format("sudo su opensearch -s /bin/sh -c 'test -r %s && echo true || echo false'", path);
        return Boolean.parseBoolean(executeCommand(command, "Failed to check read permission"));
    }

    private boolean checkPathWritable(String path) throws IOException, InterruptedException {
        String command = String.format("sudo su opensearch -s /bin/sh -c 'test -w %s && echo true || echo false'", path);
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

    @Test
    public void testMaxProcesses() throws IOException, InterruptedException {
        String limits = executeCommand("sudo su -c 'cat /proc/" + opensearchPid + "/limits'", "Failed to read process limits");
        assertTrue("Max processes limit should be 4096 or unlimited", 
                limits.contains("Max processes             4096                 4096") ||
                limits.contains("Max processes             unlimited            unlimited"));
    }

    @Test
    public void testFileDescriptorLimit() throws IOException, InterruptedException {
        String limits = executeCommand("sudo su -c 'cat /proc/" + opensearchPid + "/limits'", "Failed to read process limits");
        assertTrue("File descriptor limit should be at least 65535", 
                limits.contains("Max open files            65535                65535") ||
                limits.contains("Max open files            unlimited            unlimited"));
    }

    @Test
    public void testSystemCallFilter() throws IOException, InterruptedException {
        // Check if Seccomp is enabled
        String seccomp = executeCommand("sudo su -c 'grep Seccomp /proc/" + opensearchPid + "/status'", "Failed to read Seccomp status");
        assertFalse("Seccomp should be enabled", seccomp.contains("0"));
        
        // Test specific system calls that should be blocked
        String rebootResult = executeCommand("sudo su opensearch -c 'kill -s SIGHUP 1' 2>&1 || echo 'Operation not permitted'", "Failed to test reboot system call");
        assertTrue("Reboot system call should be blocked", rebootResult.contains("Operation not permitted"));
        
        String swapResult = executeCommand("sudo su opensearch -c 'swapon -a' 2>&1 || echo 'Operation not permitted'", "Failed to test swap system call");
        assertTrue("Swap system call should be blocked", swapResult.contains("Operation not permitted"));
    }

    @Test
    public void testOpenSearchProcessCannotExit() throws IOException, InterruptedException {

        String scriptContent = "#!/bin/sh\n" +
                            "if [ $# -ne 1 ]; then\n" +
                            "    echo \"Usage: $0 <PID>\"\n" +
                            "    exit 1\n" +
                            "fi\n" +
                            "if kill -15 $1 2>/dev/null; then\n" +
                            "    echo \"SIGTERM signal sent to process $1\"\n" +
                            "else\n" +
                            "    echo \"Failed to send SIGTERM to process $1\"\n" +
                            "fi\n"

        String[] command = {
            "echo '" + scriptContent.replace("'", "'\"'\"'") + "' > /tmp/terminate.sh && chmod +x /tmp/terminate.sh && /tmp/terminate.sh " + opensearchPid
        };

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();

        // Wait a moment for any potential termination to take effect
        Thread.sleep(2000);

        // Verify the OpenSearch service status
        String serviceStatus = executeCommand(
            "systemctl is-active opensearch",
            "Failed to check OpenSearch service status"
        );

        assertTrue("OpenSearch process should still be running", processCheck.contains("Running"));
        assertEquals("OpenSearch service should be active", "active", serviceStatus.trim());
    }

}

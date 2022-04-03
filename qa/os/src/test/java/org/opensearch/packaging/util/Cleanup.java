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

package org.opensearch.packaging.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.opensearch.packaging.test.PackagingTestCase.getRootTempDir;
import static org.opensearch.packaging.util.FileUtils.lsGlob;
import static org.opensearch.packaging.util.Platforms.isDPKG;
import static org.opensearch.packaging.util.Platforms.isRPM;
import static org.opensearch.packaging.util.Platforms.isSystemd;

public class Cleanup {

    private static final List<String> OPENSEARCH_FILES_LINUX = Arrays.asList(
        "/usr/share/opensearch",
        "/etc/opensearch/opensearch.keystore",
        "/etc/opensearch",
        "/var/lib/opensearch",
        "/var/log/opensearch",
        "/etc/default/opensearch",
        "/etc/sysconfig/opensearch",
        "/var/run/opensearch",
        "/usr/share/doc/opensearch",
        "/usr/lib/systemd/system/opensearch.conf",
        "/usr/lib/tmpfiles.d/opensearch.conf",
        "/usr/lib/sysctl.d/opensearch.conf"
    );

    // todo
    private static final List<String> OPENSEARCH_FILES_WINDOWS = Collections.emptyList();

    public static void cleanEverything() throws Exception {
        final Shell sh = new Shell();

        // kill opensearch processes
        Platforms.onLinux(() -> {
            sh.runIgnoreExitCode("pkill -u opensearch");
            sh.runIgnoreExitCode("ps aux | grep -i 'org.opensearch.bootstrap.OpenSearch' | awk {'print $2'} | xargs kill -9");
        });

        Platforms.onWindows(() -> {
            // the view of processes returned by Get-Process doesn't expose command line arguments, so we use WMI here
            sh.runIgnoreExitCode(
                "Get-WmiObject Win32_Process | "
                    + "Where-Object { $_.CommandLine -Match 'org.opensearch.bootstrap.OpenSearch' } | "
                    + "ForEach-Object { $_.Terminate() }"
            );
        });

        Platforms.onLinux(Cleanup::purgePackagesLinux);

        // remove opensearch users
        Platforms.onLinux(() -> {
            sh.runIgnoreExitCode("userdel opensearch");
            sh.runIgnoreExitCode("groupdel opensearch");
        });
        // when we run es as a role user on windows, add the equivalent here

        // delete files that may still exist
        lsGlob(getRootTempDir(), "opensearch*").forEach(FileUtils::rm);
        final List<String> filesToDelete = Platforms.WINDOWS ? OPENSEARCH_FILES_WINDOWS : OPENSEARCH_FILES_LINUX;
        // windows needs leniency due to asinine releasing of file locking async from a process exiting
        Consumer<? super Path> rm = Platforms.WINDOWS ? FileUtils::rmWithRetries : FileUtils::rm;
        filesToDelete.stream().map(Paths::get).filter(Files::exists).forEach(rm);

        // disable opensearch service
        // todo add this for windows when adding tests for service intallation
        if (Platforms.LINUX && isSystemd()) {
            sh.run("systemctl unmask systemd-sysctl.service");
        }
    }

    private static void purgePackagesLinux() {
        final Shell sh = new Shell();

        if (isRPM()) {
            // Doing rpm erase on both packages in one command will remove neither since both cannot be installed
            // this may leave behind config files in /etc/opensearch, but a later step in this cleanup will get them
            sh.runIgnoreExitCode("rpm --quiet -e opensearch");
        }

        if (isDPKG()) {
            sh.runIgnoreExitCode("dpkg --purge opensearch");
        }
    }
}

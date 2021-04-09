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

package org.opensearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.opensearch.packaging.util.FileUtils;
import org.opensearch.packaging.util.Packages;
import org.opensearch.packaging.util.Shell.Result;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Collections.singletonList;
import static org.opensearch.packaging.util.FileExistenceMatchers.fileDoesNotExist;
import static org.opensearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.opensearch.packaging.util.FileUtils.append;
import static org.opensearch.packaging.util.FileUtils.assertPathsDoNotExist;
import static org.opensearch.packaging.util.FileUtils.assertPathsExist;
import static org.opensearch.packaging.util.FileUtils.fileWithGlobExist;
import static org.opensearch.packaging.util.FileUtils.mv;
import static org.opensearch.packaging.util.FileUtils.rm;
import static org.opensearch.packaging.util.FileUtils.slurp;
import static org.opensearch.packaging.util.Packages.SYSTEMD_SERVICE;
import static org.opensearch.packaging.util.Packages.assertInstalled;
import static org.opensearch.packaging.util.Packages.assertRemoved;
import static org.opensearch.packaging.util.Packages.installPackage;
import static org.opensearch.packaging.util.Packages.remove;
import static org.opensearch.packaging.util.Packages.restartOpenSearch;
import static org.opensearch.packaging.util.Packages.verifyPackageInstallation;
import static org.opensearch.packaging.util.Platforms.getOsRelease;
import static org.opensearch.packaging.util.Platforms.isSystemd;
import static org.opensearch.packaging.util.ServerUtils.makeRequest;
import static org.opensearch.packaging.util.ServerUtils.runOpenSearchTests;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

public class PackageTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("rpm or deb", distribution.isPackage());
    }

    public void test10InstallPackage() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
    }

    public void test20PluginsCommandWhenNoPlugins() {
        assertThat(sh.run(installation.bin("opensearch-plugin") + " list").stdout, is(emptyString()));
    }

    public void test30DaemonIsNotEnabledOnRestart() {
        if (isSystemd()) {
            sh.run("systemctl daemon-reload");
            String isEnabledOutput = sh.runIgnoreExitCode("systemctl is-enabled opensearch.service").stdout.trim();
            assertThat(isEnabledOutput, equalTo("disabled"));
        }
    }

    public void test31InstallDoesNotStartServer() {
        assertThat(sh.run("ps aux").stdout, not(containsString("org.opensearch.bootstrap.OpenSearch")));
    }

    private void assertRunsWithJavaHome() throws Exception {
        byte[] originalEnvFile = Files.readAllBytes(installation.envFile);
        try {
            Files.write(installation.envFile, singletonList("JAVA_HOME=" + systemJavaHome), APPEND);
            startOpenSearch();
            runOpenSearchTests();
            stopOpenSearch();
        } finally {
            Files.write(installation.envFile, originalEnvFile);
        }

        assertThat(FileUtils.slurpAllLogs(installation.logs, "opensearch.log", "opensearch*.log.gz"), containsString(systemJavaHome));
    }

    public void test32JavaHomeOverride() throws Exception {
        // we always run with java home when no bundled jdk is included, so this test would be repetitive
        assumeThat(distribution().hasJdk, is(true));

        assertRunsWithJavaHome();
    }

    public void test33RunsIfJavaNotOnPath() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        // we don't require java be installed but some images have it
        String backupPath = "/usr/bin/java." + getClass().getSimpleName() + ".bak";
        if (Files.exists(Paths.get("/usr/bin/java"))) {
            sh.run("sudo mv /usr/bin/java " + backupPath);
        }

        try {
            startOpenSearch();
            runOpenSearchTests();
            stopOpenSearch();
        } finally {
            if (Files.exists(Paths.get(backupPath))) {
                sh.run("sudo mv " + backupPath + " /usr/bin/java");
            }
        }
    }

    public void test34CustomJvmOptionsDirectoryFile() throws Exception {
        final Path heapOptions = installation.config(Paths.get("jvm.options.d", "heap.options"));
        try {
            append(heapOptions, "-Xms512m\n-Xmx512m\n");

            startOpenSearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));

            stopOpenSearch();
        } finally {
            rm(heapOptions);
        }
    }

    public void test42BundledJdkRemoved() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");
        try {
            mv(installation.bundledJdk, relocatedJdk);
            assertRunsWithJavaHome();
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test40StartServer() throws Exception {
        String start = sh.runIgnoreExitCode("date ").stdout.trim();
        startOpenSearch();

        String journalEntries = sh.runIgnoreExitCode(
            "journalctl _SYSTEMD_UNIT=opensearch.service "
                + "--since \""
                + start
                + "\" --output cat | grep -v \"future versions of OpenSearch will require Java 11\" | wc -l"
        ).stdout.trim();
        assertThat(journalEntries, equalTo("0"));

        assertPathsExist(installation.pidDir.resolve("opensearch.pid"));
        assertPathsExist(installation.logs.resolve("opensearch_server.json"));

        runOpenSearchTests();
        verifyPackageInstallation(installation, distribution(), sh); // check startup script didn't change permissions
        stopOpenSearch();
    }

    public void test50Remove() throws Exception {
        // add fake bin directory as if a plugin was installed
        Files.createDirectories(installation.bin.resolve("myplugin"));

        remove(distribution());

        // removing must stop the service
        assertThat(sh.run("ps aux").stdout, not(containsString("org.opensearch.bootstrap.OpenSearch")));

        if (isSystemd()) {

            final int statusExitCode;

            // Before version 231 systemctl returned exit code 3 for both services that were stopped, and nonexistent
            // services [1]. In version 231 and later it returns exit code 4 for non-existent services.
            //
            // The exception is Centos 7 and oel 7 where it returns exit code 4 for non-existent services from a systemd reporting a version
            // earlier than 231. Centos 6 does not have an /etc/os-release, but that's fine because it also doesn't use systemd.
            //
            // [1] https://github.com/systemd/systemd/pull/3385
            if (getOsRelease().contains("ID=\"centos\"") || getOsRelease().contains("ID=\"ol\"")) {
                statusExitCode = 4;
            } else {

                final Result versionResult = sh.run("systemctl --version");
                final Matcher matcher = Pattern.compile("^systemd (\\d+)").matcher(versionResult.stdout);
                matcher.find();
                final int version = Integer.parseInt(matcher.group(1));

                statusExitCode = version < 231 ? 3 : 4;
            }

            assertThat(sh.runIgnoreExitCode("systemctl status opensearch.service").exitCode, is(statusExitCode));
            assertThat(sh.runIgnoreExitCode("systemctl is-enabled opensearch.service").exitCode, is(1));

        }

        assertPathsDoNotExist(
            installation.bin,
            installation.lib,
            installation.modules,
            installation.plugins,
            installation.logs,
            installation.pidDir
        );

        assertThat(SYSTEMD_SERVICE, fileDoesNotExist());
    }

    public void test60Reinstall() throws Exception {
        install();
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);

        remove(distribution());
        assertRemoved(distribution());
    }

    public void test70RestartServer() throws Exception {
        try {
            install();
            assertInstalled(distribution());

            startOpenSearch();
            restartOpenSearch(sh, installation);
            runOpenSearchTests();
            stopOpenSearch();
        } finally {
            cleanup();
        }
    }

    public void test72TestRuntimeDirectory() throws Exception {
        try {
            install();
            FileUtils.rm(installation.pidDir);
            startOpenSearch();
            assertPathsExist(installation.pidDir);
            stopOpenSearch();
        } finally {
            cleanup();
        }
    }

    public void test73gcLogsExist() throws Exception {
        install();
        startOpenSearch();
        // it can be gc.log or gc.log.0.current
        assertThat(installation.logs, fileWithGlobExist("gc.log*"));
        stopOpenSearch();
    }

    // TEST CASES FOR SYSTEMD ONLY

    /**
     * # Simulates the behavior of a system restart:
     * # the PID directory is deleted by the operating system
     * # but it should not block ES from starting
     * # see https://github.com/elastic/elasticsearch/issues/11594
     */
    public void test80DeletePID_DIRandRestart() throws Exception {
        assumeTrue(isSystemd());

        rm(installation.pidDir);

        sh.run("systemd-tmpfiles --create");

        startOpenSearch();

        final Path pidFile = installation.pidDir.resolve("opensearch.pid");

        assertThat(pidFile, fileExists());

        stopOpenSearch();
    }

    public void test81CustomPathConfAndJvmOptions() throws Exception {
        assumeTrue(isSystemd());

        assertPathsExist(installation.envFile);
        stopOpenSearch();

        withCustomConfig(tempConf -> {
            append(installation.envFile, "OPENSEARCH_JAVA_OPTS=-XX:-UseCompressedOops");

            startOpenSearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":1073741824"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));

            stopOpenSearch();
        });

        cleanup();
    }

    public void test83SystemdMask() throws Exception {
        try {
            assumeTrue(isSystemd());

            sh.run("systemctl mask systemd-sysctl.service");
            install();

            sh.run("systemctl unmask systemd-sysctl.service");
        } finally {
            cleanup();
        }
    }

    public void test84serviceFileSetsLimits() throws Exception {
        // Limits are changed on systemd platforms only
        assumeTrue(isSystemd());

        install();

        startOpenSearch();

        final Path pidFile = installation.pidDir.resolve("opensearch.pid");
        assertThat(pidFile, fileExists());
        String pid = slurp(pidFile).trim();
        String maxFileSize = sh.run("cat /proc/%s/limits | grep \"Max file size\" | awk '{ print $4 }'", pid).stdout.trim();
        assertThat(maxFileSize, equalTo("unlimited"));

        String maxProcesses = sh.run("cat /proc/%s/limits | grep \"Max processes\" | awk '{ print $3 }'", pid).stdout.trim();
        assertThat(maxProcesses, equalTo("4096"));

        String maxOpenFiles = sh.run("cat /proc/%s/limits | grep \"Max open files\" | awk '{ print $4 }'", pid).stdout.trim();
        assertThat(maxOpenFiles, equalTo("65535"));

        String maxAddressSpace = sh.run("cat /proc/%s/limits | grep \"Max address space\" | awk '{ print $4 }'", pid).stdout.trim();
        assertThat(maxAddressSpace, equalTo("unlimited"));

        stopOpenSearch();
    }

    public void test90DoNotCloseStderrWhenQuiet() throws Exception {
        assumeTrue(isSystemd());

        assertPathsExist(installation.envFile);
        stopOpenSearch();

        withCustomConfig(tempConf -> {
            // Create a startup problem by adding an invalid YAML line to the config
            append(tempConf.resolve("opensearch.yml"), "discovery.zen.ping.unicast.hosts:15172.30.5.3416172.30.5.35, 172.30.5.17]\n");

            // Make sure we don't pick up the journal entries for previous ES instances.
            Packages.JournaldWrapper journald = new Packages.JournaldWrapper(sh);
            runOpenSearchStartCommand(null, true, false);
            final Result logs = journald.getLogs();

            assertThat(logs.stdout, containsString("Failed to load settings from [opensearch.yml]"));
        });
    }
}

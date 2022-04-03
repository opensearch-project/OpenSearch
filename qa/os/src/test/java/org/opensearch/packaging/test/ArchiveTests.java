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
import org.opensearch.packaging.util.Installation;
import org.opensearch.packaging.util.Platforms;
import org.opensearch.packaging.util.ServerUtils;
import org.opensearch.packaging.util.Shell.Result;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.opensearch.packaging.util.Archives.installArchive;
import static org.opensearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.opensearch.packaging.util.FileUtils.append;
import static org.opensearch.packaging.util.FileUtils.mv;
import static org.opensearch.packaging.util.FileUtils.rm;
import static org.opensearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

public class ArchiveTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only archives", distribution.isArchive());
    }

    public void test10Install() throws Exception {
        installation = installArchive(sh, distribution());
        verifyArchiveInstallation(installation, distribution());
    }

    public void test20PluginsListWithNoPlugins() throws Exception {
        final Installation.Executables bin = installation.executables();
        final Result r = bin.pluginTool.run("list");

        assertThat(r.stdout, emptyString());
    }

    public void test30MissingBundledJdk() throws Exception {
        final Installation.Executables bin = installation.executables();
        sh.getEnv().remove("JAVA_HOME");
        sh.getEnv().remove("OPENSEARCH_JAVA_HOME");

        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");

        try {
            if (distribution().hasJdk) {
                mv(installation.bundledJdk, relocatedJdk);
            }
            // ask for opensearch version to quickly exit if java is actually found (ie test failure)
            final Result runResult = sh.runIgnoreExitCode(bin.opensearch.toString() + " -v");
            assertThat(runResult.exitCode, is(1));
            assertThat(runResult.stderr, containsString("could not find java in bundled jdk"));
        } finally {
            if (distribution().hasJdk) {
                mv(relocatedJdk, installation.bundledJdk);
            }
        }
    }

    public void test31BadJavaHome() throws Exception {
        final Installation.Executables bin = installation.executables();
        sh.getEnv().remove("OPENSEARCH_JAVA_HOME");
        sh.getEnv().put("JAVA_HOME", "doesnotexist");

        // ask for opensearch version to quickly exit if java is actually found (ie test failure)
        final Result runResult = sh.runIgnoreExitCode(bin.opensearch.toString() + " -V");
        assertThat(runResult.exitCode, is(1));
        assertThat(runResult.stderr, containsString("could not find java in JAVA_HOME"));

    }

    public void test31BadOpensearchJavaHome() throws Exception {
        final Installation.Executables bin = installation.executables();
        sh.getEnv().put("OPENSEARCH_JAVA_HOME", "doesnotexist");

        // ask for opensearch version to quickly exit if java is actually found (ie test failure)
        final Result runResult = sh.runIgnoreExitCode(bin.opensearch.toString() + " -V");
        assertThat(runResult.exitCode, is(1));
        assertThat(runResult.stderr, containsString("could not find java in OPENSEARCH_JAVA_HOME"));

    }

    public void test32SpecialCharactersInJdkPath() throws Exception {
        final Installation.Executables bin = installation.executables();
        assumeTrue("Only run this test when we know where the JDK is.", distribution().hasJdk);

        final Path relocatedJdk = installation.bundledJdk.getParent().resolve("a (special) path");
        sh.getEnv().remove("OPENSEARCH_JAVA_HOME");
        sh.getEnv().put("JAVA_HOME", relocatedJdk.toString());

        try {
            mv(installation.bundledJdk, relocatedJdk);
            // ask for opensearch version to avoid starting the app
            final Result runResult = sh.run(bin.opensearch.toString() + " -V");
            assertThat(runResult.stdout, startsWith("Version: "));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test50StartAndStop() throws Exception {
        // cleanup from previous test
        rm(installation.config("opensearch.keystore"));

        try {
            startOpenSearch();
        } catch (Exception e) {
            if (Files.exists(installation.home.resolve("opensearch.pid"))) {
                String pid = FileUtils.slurp(installation.home.resolve("opensearch.pid")).trim();
                logger.info("Dumping jstack of opensearch processb ({}) that failed to start", pid);
                sh.runIgnoreExitCode("jstack " + pid);
            }
            throw e;
        }

        List<Path> gcLogs = FileUtils.lsGlob(installation.logs, "gc.log*");
        assertThat(gcLogs, is(not(empty())));
        ServerUtils.runOpenSearchTests();

        stopOpenSearch();
    }

    public void test51JavaHomeOverride() throws Exception {
        sh.getEnv().remove("OPENSEARCH_JAVA_HOME");

        Platforms.onLinux(() -> {
            String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome1);
        });
        Platforms.onWindows(() -> {
            final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("JAVA_HOME", systemJavaHome1);
        });

        startOpenSearch();
        ServerUtils.runOpenSearchTests();
        stopOpenSearch();

        String systemJavaHome1 = sh.getEnv().get("JAVA_HOME");
        assertThat(FileUtils.slurpAllLogs(installation.logs, "opensearch.log", "*.log.gz"), containsString(systemJavaHome1));
    }

    public void test51OpensearchJavaHomeOverride() throws Exception {
        Platforms.onLinux(() -> {
            String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("OPENSEARCH_JAVA_HOME", systemJavaHome1);
            sh.getEnv().put("JAVA_HOME", "doesnotexist");
        });
        Platforms.onWindows(() -> {
            final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
            sh.getEnv().put("OPENSEARCH_JAVA_HOME", systemJavaHome1);
            sh.getEnv().put("JAVA_HOME", "doesnotexist");
        });

        startOpenSearch();
        ServerUtils.runOpenSearchTests();
        stopOpenSearch();

        String systemJavaHome1 = sh.getEnv().get("OPENSEARCH_JAVA_HOME");
        assertThat(FileUtils.slurpAllLogs(installation.logs, "opensearch.log", "*.log.gz"), containsString(systemJavaHome1));
    }

    public void test52JavaHomeBundledJdkRemoved() throws Exception {
        assumeThat(distribution().hasJdk, is(true));
        sh.getEnv().remove("OPENSEARCH_JAVA_HOME");

        Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");
        try {
            mv(installation.bundledJdk, relocatedJdk);
            Platforms.onLinux(() -> {
                String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
                sh.getEnv().put("JAVA_HOME", systemJavaHome1);
            });
            Platforms.onWindows(() -> {
                final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
                sh.getEnv().put("JAVA_HOME", systemJavaHome1);
            });

            startOpenSearch();
            ServerUtils.runOpenSearchTests();
            stopOpenSearch();

            String systemJavaHome1 = sh.getEnv().get("JAVA_HOME");
            assertThat(FileUtils.slurpAllLogs(installation.logs, "opensearch.log", "*.log.gz"), containsString(systemJavaHome1));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test52OpensearchJavaHomeBundledJdkRemoved() throws Exception {
        assumeThat(distribution().hasJdk, is(true));

        Path relocatedJdk = installation.bundledJdk.getParent().resolve("jdk.relocated");
        try {
            mv(installation.bundledJdk, relocatedJdk);
            Platforms.onLinux(() -> {
                String systemJavaHome1 = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
                sh.getEnv().put("OPENSEARCH_JAVA_HOME", systemJavaHome1);
                sh.getEnv().put("JAVA_HOME", "doesnotexist");
            });
            Platforms.onWindows(() -> {
                final String systemJavaHome1 = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
                sh.getEnv().put("OPENSEARCH_JAVA_HOME", systemJavaHome1);
                sh.getEnv().put("JAVA_HOME", "doesnotexist");
            });

            startOpenSearch();
            ServerUtils.runOpenSearchTests();
            stopOpenSearch();

            String systemJavaHome1 = sh.getEnv().get("OPENSEARCH_JAVA_HOME");
            assertThat(FileUtils.slurpAllLogs(installation.logs, "opensearch.log", "*.log.gz"), containsString(systemJavaHome1));
        } finally {
            mv(relocatedJdk, installation.bundledJdk);
        }
    }

    public void test53JavaHomeWithSpecialCharacters() throws Exception {
        sh.getEnv().remove("OPENSEARCH_JAVA_HOME");

        Platforms.onWindows(() -> {
            String javaPath = "C:\\Program Files (x86)\\java";
            try {
                // once windows 2012 is no longer supported and powershell 5.0 is always available we can change this command
                sh.run("cmd /c mklink /D '" + javaPath + "' $Env:SYSTEM_JAVA_HOME");

                sh.getEnv().put("JAVA_HOME", "C:\\Program Files (x86)\\java");

                // verify ES can start, stop and run plugin list
                startOpenSearch();

                stopOpenSearch();

                String pluginListCommand = installation.bin + "/opensearch-plugin list";
                Result result = sh.run(pluginListCommand);
                assertThat(result.exitCode, equalTo(0));

            } finally {
                // clean up sym link
                if (Files.exists(Paths.get(javaPath))) {
                    sh.run("cmd /c rmdir '" + javaPath + "' ");
                }
            }
        });

        Platforms.onLinux(() -> {
            // Create temporary directory with a space and link to real java home
            String testJavaHome = Paths.get("/tmp", "java home").toString();
            try {
                final String systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
                sh.run("ln -s \"" + systemJavaHome + "\" \"" + testJavaHome + "\"");
                sh.getEnv().put("JAVA_HOME", testJavaHome);

                // verify ES can start, stop and run plugin list
                startOpenSearch();

                stopOpenSearch();

                String pluginListCommand = installation.bin + "/opensearch-plugin list";
                Result result = sh.run(pluginListCommand);
                assertThat(result.exitCode, equalTo(0));
            } finally {
                FileUtils.rm(Paths.get(testJavaHome));
            }
        });
    }

    public void test54ForceBundledJdkEmptyJavaHome() throws Exception {
        assumeThat(distribution().hasJdk, is(true));
        // cleanup from previous test
        rm(installation.config("opensearch.keystore"));

        sh.getEnv().put("OPENSEARCH_JAVA_HOME", "");
        sh.getEnv().put("JAVA_HOME", "");

        startOpenSearch();
        ServerUtils.runOpenSearchTests();
        stopOpenSearch();
    }

    public void test70CustomPathConfAndJvmOptions() throws Exception {

        withCustomConfig(tempConf -> {
            final List<String> jvmOptions = org.opensearch.common.collect.List.of("-Xms512m", "-Xmx512m", "-Dlog4j2.disable.jmx=true");
            Files.write(tempConf.resolve("jvm.options"), jvmOptions, CREATE, APPEND);

            sh.getEnv().put("OPENSEARCH_JAVA_OPTS", "-XX:-UseCompressedOops");

            startOpenSearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));

            stopOpenSearch();
        });
    }

    public void test71CustomJvmOptionsDirectoryFile() throws Exception {
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

    public void test72CustomJvmOptionsDirectoryFilesAreProcessedInSortedOrder() throws Exception {
        final Path firstOptions = installation.config(Paths.get("jvm.options.d", "first.options"));
        final Path secondOptions = installation.config(Paths.get("jvm.options.d", "second.options"));
        try {
            /*
             * We override the heap in the first file, and disable compressed oops, and override the heap in the second file. By doing this,
             * we can test that both files are processed by the JVM options parser, and also that they are processed in lexicographic order.
             */
            append(firstOptions, "-Xms384m\n-Xmx384m\n-XX:-UseCompressedOops\n");
            append(secondOptions, "-Xms512m\n-Xmx512m\n");

            startOpenSearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":536870912"));
            assertThat(nodesResponse, containsString("\"using_compressed_ordinary_object_pointers\":\"false\""));

            stopOpenSearch();
        } finally {
            rm(firstOptions);
            rm(secondOptions);
        }
    }

    public void test73CustomJvmOptionsDirectoryFilesWithoutOptionsExtensionIgnored() throws Exception {
        final Path jvmOptionsIgnored = installation.config(Paths.get("jvm.options.d", "jvm.options.ignored"));
        try {
            append(jvmOptionsIgnored, "-Xms512\n-Xmx512m\n");

            startOpenSearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"heap_init_in_bytes\":1073741824"));

            stopOpenSearch();
        } finally {
            rm(jvmOptionsIgnored);
        }
    }

    public void test80RelativePathConf() throws Exception {

        withCustomConfig(tempConf -> {
            append(tempConf.resolve("opensearch.yml"), "node.name: relative");

            startOpenSearch();

            final String nodesResponse = makeRequest(Request.Get("http://localhost:9200/_nodes"));
            assertThat(nodesResponse, containsString("\"name\":\"relative\""));

            stopOpenSearch();
        });
    }

    public void test91OpenSearchShardCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.shardTool + " -h");
            assertThat(result.stdout, containsString("A CLI tool to remove corrupted parts of unrecoverable shards"));
        };
    }

    public void test92OpenSearchNodeCliPackaging() throws Exception {
        final Installation.Executables bin = installation.executables();

        Platforms.PlatformAction action = () -> {
            final Result result = sh.run(bin.nodeTool + " -h");
            assertThat(result.stdout, containsString("A CLI tool to do unsafe cluster and index manipulations on current node"));
        };
    }

    public void test93OpenSearchNodeCustomDataPathAndNotEsHomeWorkDir() throws Exception {
        Path relativeDataPath = installation.data.relativize(installation.home);
        append(installation.config("opensearch.yml"), "path.data: " + relativeDataPath);

        sh.setWorkingDirectory(getRootTempDir());

        startOpenSearch();
        stopOpenSearch();

        Result result = sh.run("echo y | " + installation.executables().nodeTool + " unsafe-bootstrap");
        assertThat(result.stdout, containsString("Master node was successfully bootstrapped"));
    }
}

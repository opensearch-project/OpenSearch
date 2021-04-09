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

import org.opensearch.packaging.util.Platforms;
import org.opensearch.packaging.util.ServerUtils;
import org.opensearch.packaging.util.Shell;
import org.junit.BeforeClass;

import static org.opensearch.packaging.util.FileUtils.assertPathsExist;
import static org.opensearch.packaging.util.FileUtils.fileWithGlobExist;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class SysVInitTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("rpm or deb", distribution.isPackage());
        assumeTrue(Platforms.isSysVInit());
        assumeFalse(Platforms.isSystemd());
    }

    @Override
    public void startOpenSearch() throws Exception {
        sh.run("service opensearch start");
        ServerUtils.waitForOpenSearch(installation);
        sh.run("service opensearch status");
    }

    @Override
    public void stopOpenSearch() {
        sh.run("service opensearch stop");
    }

    public void test10Install() throws Exception {
        install();
    }

    public void test20Start() throws Exception {
        startOpenSearch();
        assertThat(installation.logs, fileWithGlobExist("gc.log*"));
        ServerUtils.runOpenSearchTests();
        sh.run("service opensearch status"); // returns 0 exit status when ok
    }

    public void test21Restart() throws Exception {
        sh.run("service opensearch restart");
        sh.run("service opensearch status"); // returns 0 exit status when ok
    }

    public void test22Stop() throws Exception {
        stopOpenSearch();
        Shell.Result status = sh.runIgnoreExitCode("service opensearch status");
        assertThat(status.exitCode, anyOf(equalTo(3), equalTo(4)));
    }

    public void test30PidDirCreation() throws Exception {
        // Simulates the behavior of a system restart:
        // the PID directory is deleted by the operating system
        // but it should not block ES from starting
        // see https://github.com/elastic/elasticsearch/issues/11594

        sh.run("rm -rf " + installation.pidDir);
        startOpenSearch();
        assertPathsExist(installation.pidDir.resolve("opensearch.pid"));
        stopOpenSearch();
    }

    public void test31MaxMapTooSmall() throws Exception {
        sh.run("sysctl -q -w vm.max_map_count=262140");
        startOpenSearch();
        Shell.Result result = sh.run("sysctl -n vm.max_map_count");
        String maxMapCount = result.stdout.trim();
        sh.run("service opensearch stop");
        assertThat(maxMapCount, equalTo("262144"));
    }

    public void test32MaxMapBigEnough() throws Exception {
        // Ensures that if $MAX_MAP_COUNT is greater than the set
        // value on the OS we do not attempt to update it.
        sh.run("sysctl -q -w vm.max_map_count=262145");
        startOpenSearch();
        Shell.Result result = sh.run("sysctl -n vm.max_map_count");
        String maxMapCount = result.stdout.trim();
        sh.run("service opensearch stop");
        assertThat(maxMapCount, equalTo("262145"));
    }

}

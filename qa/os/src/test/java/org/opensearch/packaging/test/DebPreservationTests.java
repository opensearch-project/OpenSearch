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

import org.opensearch.packaging.util.Distribution;
import org.junit.BeforeClass;

import java.nio.file.Paths;

import static org.opensearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.opensearch.packaging.util.FileUtils.append;
import static org.opensearch.packaging.util.FileUtils.assertPathsDoNotExist;
import static org.opensearch.packaging.util.FileUtils.assertPathsExist;
import static org.opensearch.packaging.util.Packages.SYSVINIT_SCRIPT;
import static org.opensearch.packaging.util.Packages.assertInstalled;
import static org.opensearch.packaging.util.Packages.assertRemoved;
import static org.opensearch.packaging.util.Packages.installPackage;
import static org.opensearch.packaging.util.Packages.remove;
import static org.opensearch.packaging.util.Packages.verifyPackageInstallation;
import static org.junit.Assume.assumeTrue;

public class DebPreservationTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only deb", distribution.packaging == Distribution.Packaging.DEB);
        assumeTrue("only bundled jdk", distribution.hasJdk);
    }

    public void test10Install() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
    }

    public void test20Remove() throws Exception {
        append(installation.config(Paths.get("jvm.options.d", "heap.options")), "# foo");

        remove(distribution());

        // some config files were not removed
        assertPathsExist(
            installation.config,
            installation.config("opensearch.yml"),
            installation.config("jvm.options"),
            installation.config("log4j2.properties"),
            installation.config(Paths.get("jvm.options.d", "heap.options"))
        );

        // keystore was removed

        assertPathsDoNotExist(installation.config("opensearch.keystore"), installation.config(".opensearch.keystore.initial_md5sum"));

        // sysvinit service file was not removed
        assertThat(SYSVINIT_SCRIPT, fileExists());

        // defaults file was not removed
        assertThat(installation.envFile, fileExists());
    }
}

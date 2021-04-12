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

package org.opensearch.bootstrap;


import org.opensearch.cli.ExitCodes;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class EvilOpenSearchCliTests extends OpenSearchCliTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testPathHome() throws Exception {
        final String pathHome = System.getProperty("opensearch.path.home");
        final String value = randomAlphaOfLength(16);
        System.setProperty("opensearch.path.home", value);

        runTest(
                ExitCodes.OK,
                true,
                (output, error) -> {},
                (foreground, pidFile, quiet, esSettings) -> {
                    Settings settings = esSettings.settings();
                    assertThat(settings.keySet(), hasSize(2));
                    assertThat(
                        settings.get("path.home"),
                        equalTo(PathUtils.get(System.getProperty("user.dir")).resolve(value).toString()));
                    assertThat(settings.keySet(), hasItem("path.logs")); // added by env initialization
                });

        System.clearProperty("opensearch.path.home");
        final String commandLineValue = randomAlphaOfLength(16);
        runTest(
                ExitCodes.OK,
                true,
                (output, error) -> {},
                (foreground, pidFile, quiet, esSettings) -> {
                    Settings settings = esSettings.settings();
                    assertThat(settings.keySet(), hasSize(2));
                    assertThat(
                        settings.get("path.home"),
                        equalTo(PathUtils.get(System.getProperty("user.dir")).resolve(commandLineValue).toString()));
                    assertThat(settings.keySet(), hasItem("path.logs")); // added by env initialization
                },
                "-Epath.home=" + commandLineValue);

        if (pathHome != null) System.setProperty("opensearch.path.home", pathHome);
        else System.clearProperty("opensearch.path.home");
    }

}

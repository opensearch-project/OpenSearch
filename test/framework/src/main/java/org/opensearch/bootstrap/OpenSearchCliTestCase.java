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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.opensearch.cli.MockTerminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.equalTo;

abstract class OpenSearchCliTestCase extends OpenSearchTestCase {

    interface InitConsumer {
        void accept(boolean foreground, Path pidFile, boolean quiet, Environment initialEnv);
    }

    void runTest(
        final int expectedStatus,
        final boolean expectedInit,
        final BiConsumer<String, String> outputConsumer,
        final InitConsumer initConsumer,
        final String... args
    ) throws Exception {
        final MockTerminal terminal = new MockTerminal();
        final Path home = createTempDir();
        try {
            final AtomicBoolean init = new AtomicBoolean();
            final int status = OpenSearch.main(args, new OpenSearch() {
                @Override
                protected Environment createEnv(final Map<String, String> settings) throws UserException {
                    Settings.Builder builder = Settings.builder().put("path.home", home);
                    settings.forEach((k, v) -> builder.put(k, v));
                    final Settings realSettings = builder.build();
                    return new Environment(realSettings, home.resolve("config"));
                }

                @Override
                void init(final boolean daemonize, final Path pidFile, final boolean quiet, Environment initialEnv) {
                    init.set(true);
                    initConsumer.accept(!daemonize, pidFile, quiet, initialEnv);
                }

                @Override
                protected boolean addShutdownHook() {
                    return false;
                }
            }, terminal);
            assertThat(status, equalTo(expectedStatus));
            assertThat(init.get(), equalTo(expectedInit));
            outputConsumer.accept(terminal.getOutput(), terminal.getErrorOutput());
        } catch (Exception e) {
            // if an unexpected exception is thrown, we log
            // terminal output to aid debugging
            logger.info("Stdout:\n" + terminal.getOutput());
            logger.info("Stderr:\n" + terminal.getErrorOutput());
            // rethrow so the test fails
            throw e;
        }
    }

}

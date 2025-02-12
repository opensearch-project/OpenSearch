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

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TimeUnits;
import org.opensearch.Version;
import org.opensearch.bootstrap.BootstrapForTesting;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.junit.listeners.ReproduceInfoPrinter;

@Listeners({ ReproduceInfoPrinter.class })
@TimeoutSuite(millis = TimeUnits.HOUR)
@LuceneTestCase.SuppressReproduceLine
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
/*
  Basic test case for token streams. the assertion methods in this class will
  run basic checks to enforce correct behavior of the token streams.
 */
public abstract class OpenSearchTokenStreamTestCase extends BaseTokenStreamTestCase {

    static {
        try {
            Class.forName("org.opensearch.test.OpenSearchTestCase");
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
        BootstrapForTesting.ensureInitialized();
    }

    public Settings.Builder newAnalysisSettingsBuilder() {
        return Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
    }

}

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

package org.opensearch.bwcompat;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.CorruptStateException;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

@LuceneTestCase.SuppressCodecs("*")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 0, maxNumDataNodes = 0)
public class RecoveryWithUnsupportedIndicesIT extends OpenSearchIntegTestCase {

    /**
     * Return settings that could be used to start a node that has the given zipped home directory.
     */
    private Settings prepareBackwardsDataDir(Path backwardsIndex) throws IOException {
        Path indexDir = createTempDir();
        Path dataDir = indexDir.resolve("data");
        try (InputStream stream = Files.newInputStream(backwardsIndex)) {
            TestUtil.unzip(stream, indexDir);
        }
        assertTrue(Files.exists(dataDir));

        // list clusters in the datapath, ignoring anything from extrasfs
        final Path[] list;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir)) {
            List<Path> dirs = new ArrayList<>();
            for (Path p : stream) {
                if (!p.getFileName().toString().startsWith("extra")) {
                    dirs.add(p);
                }
            }
            list = dirs.toArray(new Path[0]);
        }

        if (list.length != 1) {
            StringBuilder builder = new StringBuilder("Backwards index must contain exactly one cluster\n");
            for (Path line : list) {
                builder.append(line.toString()).append('\n');
            }
            throw new IllegalStateException(builder.toString());
        }
        Path src = list[0].resolve(NodeEnvironment.NODES_FOLDER);
        Path dest = dataDir.resolve(NodeEnvironment.NODES_FOLDER);
        assertTrue(Files.exists(src));
        Files.move(src, dest);
        assertFalse(Files.exists(src));
        assertTrue(Files.exists(dest));
        Settings.Builder builder = Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), dataDir.toAbsolutePath());

        return builder.build();
    }

    public void testUpgradeStartClusterOn_2_4_5() throws Exception {
        String indexName = "unsupported-2.4.5";

        logger.info("Checking static index {}", indexName);
        Settings nodeSettings = prepareBackwardsDataDir(getDataPath("/indices/bwc").resolve(indexName + ".zip"));
        assertThat(
            ExceptionsHelper.unwrap(
                expectThrows(Exception.class, () -> internalCluster().startNode(nodeSettings)),
                CorruptStateException.class
            ).getMessage(),
            containsString("Format version is not supported")
        );
    }
}

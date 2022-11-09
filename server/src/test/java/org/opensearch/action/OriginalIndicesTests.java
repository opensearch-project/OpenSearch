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

package org.opensearch.action;

import org.opensearch.Version;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.test.VersionUtils.randomCompatibleVersion;
import static org.hamcrest.CoreMatchers.equalTo;

public class OriginalIndicesTests extends OpenSearchTestCase {

    private static final IndicesOptions[] indicesOptionsValues = new IndicesOptions[] {
        IndicesOptions.lenientExpandOpen(),
        IndicesOptions.strictExpand(),
        IndicesOptions.strictExpandOpen(),
        IndicesOptions.strictExpandOpenAndForbidClosed(),
        IndicesOptions.strictSingleIndexNoExpandForbidClosed() };

    public void testOriginalIndicesSerialization() throws IOException {
        int iterations = iterations(10, 30);
        for (int i = 0; i < iterations; i++) {
            OriginalIndices originalIndices = randomOriginalIndices();

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(randomCompatibleVersion(random(), Version.CURRENT));
            OriginalIndices.writeOriginalIndices(originalIndices, out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(out.getVersion());
            OriginalIndices originalIndices2 = OriginalIndices.readOriginalIndices(in);

            assertThat(originalIndices2.indices(), equalTo(originalIndices.indices()));
            assertThat(originalIndices2.indicesOptions(), equalTo(originalIndices.indicesOptions()));
        }
    }

    public static OriginalIndices randomOriginalIndices() {
        int numIndices = randomInt(10);
        String[] indices = new String[numIndices];
        for (int j = 0; j < indices.length; j++) {
            indices[j] = randomAlphaOfLength(randomIntBetween(1, 10));
        }
        IndicesOptions indicesOptions = randomFrom(indicesOptionsValues);
        return new OriginalIndices(indices, indicesOptions);
    }
}

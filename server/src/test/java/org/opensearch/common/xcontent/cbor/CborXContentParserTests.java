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

package org.opensearch.common.xcontent.cbor;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class CborXContentParserTests extends OpenSearchTestCase {
    public void testEmptyValue() throws IOException {
        BytesReference ref = BytesReference.bytes(XContentFactory.cborBuilder().startObject().field("field", "").endObject());

        for (int i = 0; i < 2; i++) {
            // Running this part twice triggers the issue.
            // See https://github.com/elastic/elasticsearch/issues/8629
            try (XContentParser parser = createParser(CborXContent.cborXContent, ref)) {
                while (parser.nextToken() != null) {
                    parser.charBuffer();
                }
            }
        }
    }
}

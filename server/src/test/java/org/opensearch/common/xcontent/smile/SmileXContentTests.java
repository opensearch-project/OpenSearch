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

package org.opensearch.common.xcontent.smile;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import org.opensearch.common.xcontent.BaseXContentTestCase;
import org.opensearch.common.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
public class SmileXContentTests extends BaseXContentTestCase {

    @Override
    public XContentType xcontentType() {
        return XContentType.SMILE;
    }

    public void testBigInteger() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        JsonGenerator generator = new SmileFactory().createGenerator(os);
        doTestBigInteger(generator, os);
    }

    public void testInvalidSurrogatePair() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        SmileFactory smileFactory = new SmileFactory();
        JsonGenerator generator = smileFactory.createGenerator(os);
        // default case throws an exception
        expectThrows(JsonGenerationException.class, () -> generator.writeFieldName("ퟀ\uDD6Dlog_processeddetail٣{r"));

        // LENIENT_UTF_ENCODING doesn't throw any exception
        smileFactory.configure(SmileGenerator.Feature.LENIENT_UTF_ENCODING, true);
        JsonGenerator generator2 = smileFactory.createGenerator(os);
        doTestInvalidSurrogatePair(generator2, os);
    }
}

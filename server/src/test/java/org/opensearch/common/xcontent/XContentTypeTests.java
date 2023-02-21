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

package org.opensearch.common.xcontent;

import org.opensearch.core.xcontent.MediaType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class XContentTypeTests extends OpenSearchTestCase {

    public void testFromJson() throws Exception {
        String mediaType = "application/json";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromNdJson() throws Exception {
        String mediaType = "application/x-ndjson";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromJsonUppercase() throws Exception {
        String mediaType = "application/json".toUpperCase(Locale.ROOT);
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromYaml() throws Exception {
        String mediaType = "application/yaml";
        XContentType expectedXContentType = XContentType.YAML;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromSmile() throws Exception {
        String mediaType = "application/smile";
        XContentType expectedXContentType = XContentType.SMILE;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromCbor() throws Exception {
        String mediaType = "application/cbor";
        XContentType expectedXContentType = XContentType.CBOR;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcard() throws Exception {
        String mediaType = "application/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcardUppercase() throws Exception {
        String mediaType = "APPLICATION/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromRubbish() throws Exception {
        assertThat(XContentType.fromMediaType((String) null), nullValue());
        assertThat(XContentType.fromMediaType((MediaType) null), nullValue());
        assertThat(XContentType.fromMediaType(""), nullValue());
        assertThat(XContentType.fromMediaType("text/plain"), nullValue());
        assertThat(XContentType.fromMediaType("gobbly;goop"), nullValue());
    }

    public void testVersionedMediaType() throws Exception {
        assertThat(XContentType.fromMediaType("application/vnd.opensearch+json;compatible-with=7"), equalTo(XContentType.JSON));
        assertThat(XContentType.fromMediaType("application/vnd.opensearch+yaml;compatible-with=7"), equalTo(XContentType.YAML));
        assertThat(XContentType.fromMediaType("application/vnd.opensearch+cbor;compatible-with=7"), equalTo(XContentType.CBOR));
        assertThat(XContentType.fromMediaType("application/vnd.opensearch+smile;compatible-with=7"), equalTo(XContentType.SMILE));

        assertThat(XContentType.fromMediaType("application/vnd.opensearch+json ;compatible-with=7"), equalTo(XContentType.JSON));

        String mthv = "application/vnd.opensearch+json ;compatible-with=7;charset=utf-8";
        assertThat(XContentType.fromMediaType(mthv), equalTo(XContentType.JSON));
        assertThat(XContentType.fromMediaType(mthv.toUpperCase(Locale.ROOT)), equalTo(XContentType.JSON));
    }
}

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

package org.opensearch.client.indices;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class PutIndexTemplateRequestTests extends AbstractXContentTestCase<PutIndexTemplateRequest> {
    public void testValidateErrorMessage() throws Exception {
        expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest(null));
        expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest("test").name(null));
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("test");
        ActionRequestValidationException withoutPattern = request.validate();
        assertThat(withoutPattern.getMessage(), containsString("index patterns are missing"));

        request.name("foo");
        ActionRequestValidationException withoutIndexPatterns = request.validate();
        assertThat(withoutIndexPatterns.validationErrors(), hasSize(1));
        assertThat(withoutIndexPatterns.getMessage(), containsString("index patterns are missing"));

        request.patterns(Collections.singletonList("test-*"));
        ActionRequestValidationException noError = request.validate();
        assertThat(noError, is(nullValue()));
    }

    @Override
    protected PutIndexTemplateRequest createTestInstance() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("test");
        if (randomBoolean()) {
            request.version(randomInt());
        }
        if (randomBoolean()) {
            request.order(randomInt());
        }
        request.patterns(Arrays.asList(generateRandomStringArray(20, 100, false, false)));
        int numAlias = between(0, 5);
        for (int i = 0; i < numAlias; i++) {
            // some ASCII or Latin-1 control characters, especially newline, can lead to
            // problems with yaml parsers, that's why we filter them here (see #30911)
            Alias alias = new Alias(randomRealisticUnicodeOfLengthBetween(1, 10).replaceAll("\\p{Cc}", ""));
            if (randomBoolean()) {
                alias.indexRouting(randomRealisticUnicodeOfLengthBetween(1, 10));
            }
            if (randomBoolean()) {
                alias.searchRouting(randomRealisticUnicodeOfLengthBetween(1, 10));
            }
            request.alias(alias);
        }
        if (randomBoolean()) {
            try {
                request.mapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject("field-" + randomInt())
                        .field("type", randomFrom("keyword", "text"))
                        .endObject()
                        .endObject()
                        .endObject()
                );
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        if (randomBoolean()) {
            request.settings(Settings.builder().put("setting1", randomLong()).put("setting2", randomTimeValue()).build());
        }
        return request;
    }

    @Override
    protected PutIndexTemplateRequest doParseInstance(XContentParser parser) throws IOException {
        return new PutIndexTemplateRequest("test").source(parser.map());
    }

    @Override
    protected void assertEqualInstances(PutIndexTemplateRequest expected, PutIndexTemplateRequest actual) {
        assertNotSame(expected, actual);
        assertThat(actual.version(), equalTo(expected.version()));
        assertThat(actual.order(), equalTo(expected.order()));
        assertThat(actual.patterns(), equalTo(expected.patterns()));
        assertThat(actual.aliases(), equalTo(expected.aliases()));
        assertThat(actual.mappings(), equalTo(expected.mappings()));
        assertThat(actual.settings(), equalTo(expected.settings()));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}

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

package org.opensearch.common.xcontent.support.filtering;

import org.junit.Assert;
import org.opensearch.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.AbstractFilteringTestCase;

import java.io.IOException;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractXContentFilteringTestCase extends AbstractFilteringTestCase {

    protected final void testFilter(Builder expected, Builder actual, Set<String> includes, Set<String> excludes) throws IOException {
        assertFilterResult(expected.apply(createBuilder()), actual.apply(createBuilder(includes, excludes)));
    }

    protected abstract void assertFilterResult(XContentBuilder expected, XContentBuilder actual);

    protected abstract XContentType getXContentType();

    private XContentBuilder createBuilder() throws IOException {
        return XContentBuilder.builder(getXContentType().xContent());
    }

    private XContentBuilder createBuilder(Set<String> includes, Set<String> excludes) throws IOException {
        return XContentBuilder.builder(getXContentType().xContent(), includes, excludes);
    }

    public void testSingleFieldObject() throws IOException {
        final Builder sample = builder -> builder.startObject().startObject("foo").field("bar", "test").endObject().endObject();

        Builder expected = builder -> builder.startObject().startObject("foo").field("bar", "test").endObject().endObject();
        testFilter(expected, sample, singleton("foo.bar"), emptySet());
        testFilter(expected, sample, emptySet(), singleton("foo.baz"));
        testFilter(expected, sample, singleton("foo"), singleton("foo.baz"));

        expected = builder -> builder.startObject().endObject();
        testFilter(expected, sample, emptySet(), singleton("foo.bar"));
        testFilter(expected, sample, singleton("foo"), singleton("foo.b*"));
    }

    static void assertXContentBuilderAsString(final XContentBuilder expected, final XContentBuilder actual) {
        Assert.assertThat(Strings.toString(actual), is(Strings.toString(expected)));
    }

    static void assertXContentBuilderAsBytes(final XContentBuilder expected, final XContentBuilder actual) {
        XContent xContent = actual.contentType().xContent();
        try (
            XContentParser jsonParser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(expected).streamInput()
            );
            XContentParser testParser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(actual).streamInput()
            );
        ) {
            while (true) {
                XContentParser.Token token1 = jsonParser.nextToken();
                XContentParser.Token token2 = testParser.nextToken();
                if (token1 == null) {
                    Assert.assertThat(token2, nullValue());
                    return;
                }
                Assert.assertThat(token1, equalTo(token2));
                switch (token1) {
                    case FIELD_NAME:
                        Assert.assertThat(jsonParser.currentName(), equalTo(testParser.currentName()));
                        break;
                    case VALUE_STRING:
                        Assert.assertThat(jsonParser.text(), equalTo(testParser.text()));
                        break;
                    case VALUE_NUMBER:
                        Assert.assertThat(jsonParser.numberType(), equalTo(testParser.numberType()));
                        Assert.assertThat(jsonParser.numberValue(), equalTo(testParser.numberValue()));
                        break;
                }
            }
        } catch (Exception e) {
            Assert.fail("Fail to verify the result of the XContentBuilder: " + e.getMessage());
        }
    }
}

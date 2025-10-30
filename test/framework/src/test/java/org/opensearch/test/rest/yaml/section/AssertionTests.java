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

package org.opensearch.test.rest.yaml.section;

import org.opensearch.common.xcontent.yaml.YamlXContent;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class AssertionTests extends AbstractClientYamlTestFragmentParserTestCase {
    public void testParseIsTrue() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "get.fields._timestamp");

        IsTrueAssertion trueAssertion = IsTrueAssertion.parse(parser);

        assertThat(trueAssertion, notNullValue());
        assertThat(trueAssertion.getField(), equalTo("get.fields._timestamp"));
    }

    public void testParseIsFalse() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "docs.1._source");

        IsFalseAssertion falseAssertion = IsFalseAssertion.parse(parser);

        assertThat(falseAssertion, notNullValue());
        assertThat(falseAssertion.getField(), equalTo("docs.1._source"));
    }

    public void testParseGreaterThan() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ field: 3}");

        GreaterThanAssertion greaterThanAssertion = GreaterThanAssertion.parse(parser);
        assertThat(greaterThanAssertion, notNullValue());
        assertThat(greaterThanAssertion.getField(), equalTo("field"));
        assertThat(greaterThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) greaterThanAssertion.getExpectedValue(), equalTo(3));

        // test mixed types

        // long vs int
        greaterThanAssertion.doAssert(3000000000L, 2);

        // int vs long
        greaterThanAssertion.doAssert(3, 2L);

        // Double vs Float
        greaterThanAssertion.doAssert(3.0d, 2.0f);

        // Float vs Double
        greaterThanAssertion.doAssert(3.0f, 2.0d);

        // sanity: both as floating-point but different concrete classes
        greaterThanAssertion.doAssert(Double.valueOf(3.0), Float.valueOf(2.0f));
    }

    public void testParseLessThan() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ field: 3}");

        LessThanAssertion lessThanAssertion = LessThanAssertion.parse(parser);
        assertThat(lessThanAssertion, notNullValue());
        assertThat(lessThanAssertion.getField(), equalTo("field"));
        assertThat(lessThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) lessThanAssertion.getExpectedValue(), equalTo(3));

        // test mixed types

        // long vs int. Should throw AssertionError instead of ClassCastException
        expectThrows(AssertionError.class, "Expected AssertionError", () -> lessThanAssertion.doAssert(3000000000L, 2));

        // int vs long
        lessThanAssertion.doAssert(2, 3L);

        // Double vs Float
        lessThanAssertion.doAssert(2.0d, 3.0f);

        // Float vs Double
        lessThanAssertion.doAssert(2.0f, 3.0d);

        // Should throw AssertionError instead of ClassCastException
        expectThrows(AssertionError.class, "Expected AssertionError", () -> lessThanAssertion.doAssert(3.0f, 2.0d));
    }

    public void testParseLength() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ _id: 22}");

        LengthAssertion lengthAssertion = LengthAssertion.parse(parser);
        assertThat(lengthAssertion, notNullValue());
        assertThat(lengthAssertion.getField(), equalTo("_id"));
        assertThat(lengthAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) lengthAssertion.getExpectedValue(), equalTo(22));
    }

    public void testParseMatchSimpleIntegerValue() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ field: 10 }");

        MatchAssertion matchAssertion = MatchAssertion.parse(parser);

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("field"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) matchAssertion.getExpectedValue(), equalTo(10));
    }

    public void testParseMatchSimpleStringValue() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ foo: bar }");

        MatchAssertion matchAssertion = MatchAssertion.parse(parser);

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("foo"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(String.class));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("bar"));
    }

    public void testParseMatchArray() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{'matches': ['test_percolator_1', 'test_percolator_2']}");

        MatchAssertion matchAssertion = MatchAssertion.parse(parser);

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("matches"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(List.class));
        List<?> strings = (List<?>) matchAssertion.getExpectedValue();
        assertThat(strings.size(), equalTo(2));
        assertThat(strings.get(0).toString(), equalTo("test_percolator_1"));
        assertThat(strings.get(1).toString(), equalTo("test_percolator_2"));
    }

    @SuppressWarnings("unchecked")
    public void testParseContains() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{testKey: { someKey: someValue } }");

        ContainsAssertion containsAssertion = ContainsAssertion.parse(parser);
        assertThat(containsAssertion, notNullValue());
        assertThat(containsAssertion.getField(), equalTo("testKey"));
        assertThat(containsAssertion.getExpectedValue(), instanceOf(Map.class));
        assertThat(((Map<String, String>) containsAssertion.getExpectedValue()).get("someKey"), equalTo("someValue"));
    }

    @SuppressWarnings("unchecked")
    public void testParseMatchSourceValues() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ _source: { responses.0.hits.total: 3, foo: bar  }}");

        MatchAssertion matchAssertion = MatchAssertion.parse(parser);

        assertThat(matchAssertion, notNullValue());
        assertThat(matchAssertion.getField(), equalTo("_source"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(Map.class));
        Map<String, Object> expectedValue = (Map<String, Object>) matchAssertion.getExpectedValue();
        assertThat(expectedValue.size(), equalTo(2));
        Object o = expectedValue.get("responses.0.hits.total");
        assertThat(o, instanceOf(Integer.class));
        assertThat((Integer) o, equalTo(3));
        o = expectedValue.get("foo");
        assertThat(o, instanceOf(String.class));
        assertThat(o.toString(), equalTo("bar"));
    }
}

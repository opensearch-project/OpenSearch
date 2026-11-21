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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.NotEqualMessageBuilder;
import org.opensearch.test.hamcrest.RegexMatcher;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Represents a match assert section:
 * <p>
 *   - match:   { get.fields._routing: "5" }
 *
 */
public class MatchAssertion extends Assertion {
    public static MatchAssertion parse(XContentParser parser) throws IOException {
        XContentLocation location = parser.getTokenLocation();
        String field = null;
        Object expectedValue = null;
        Double epsilon = null;

        Map<String, Object> fields = ParserUtils.parseFields(parser);

        if (fields.size() == 1) {
            Map.Entry<String, Object> entry = fields.entrySet().iterator().next();
            field = entry.getKey();
            expectedValue = entry.getValue();
        } else if (fields.size() == 2) {
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                if ("epsilon".equals(entry.getKey())) {
                    if (entry.getValue() instanceof Number) {
                        epsilon = ((Number) entry.getValue()).doubleValue();
                    } else {
                        throw new IllegalArgumentException("epsilon must be a number");
                    }
                } else {
                    field = entry.getKey();
                    expectedValue = entry.getValue();
                }
            }
        } else {
            throw new IllegalArgumentException("match assertion must have 1 or 2 fields, but found " + fields.size());
        }

        if (field == null) {
            throw new IllegalArgumentException("match assertion must have a field to match");
        }

        return new MatchAssertion(location, field, expectedValue, epsilon);
    }

    private static final Logger logger = LogManager.getLogger(MatchAssertion.class);
    private final Double epsilon;

    public MatchAssertion(XContentLocation location, String field, Object expectedValue) {
        this(location, field, expectedValue, null);
    }

    public MatchAssertion(XContentLocation location, String field, Object expectedValue, Double epsilon) {
        super(location, field, expectedValue);
        this.epsilon = epsilon;
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        // if the value is wrapped into / it is a regexp (e.g. /s+d+/)
        if (expectedValue instanceof String) {
            String expValue = ((String) expectedValue).trim();
            if (expValue.length() > 2 && expValue.startsWith("/") && expValue.endsWith("/")) {
                assertThat(
                    "field [" + getField() + "] was expected to be of type String but is an instanceof [" + safeClass(actualValue) + "]",
                    actualValue,
                    instanceOf(String.class)
                );
                String stringValue = (String) actualValue;
                String regex = expValue.substring(1, expValue.length() - 1);
                logger.trace("assert that [{}] matches [{}]", stringValue, regex);
                assertThat(
                    "field [" + getField() + "] was expected to match the provided regex but didn't",
                    stringValue,
                    RegexMatcher.matches(regex, Pattern.COMMENTS)
                );
                return;
            }
        }

        logger.trace("assert that [{}] matches [{}] (field [{}])", actualValue, expectedValue, getField());
        if (expectedValue == null) {
            assertNull("field [" + getField() + "] should be null but was [" + actualValue + "]", actualValue);
            return;
        }
        assertNotNull("field [" + getField() + "] is null", actualValue);

        if (this.epsilon != null && this.epsilon < 0.0) {
            throw new IllegalArgumentException("epsilon must be non-negative, but was: " + this.epsilon);
        }

        if (actualValue.getClass().equals(safeClass(expectedValue)) == false) {
            if (actualValue instanceof Number && expectedValue instanceof Number) {
                if (epsilon != null) {
                    assertThat(
                        "field [" + getField() + "] doesn't match the expected value within epsilon " + epsilon,
                        ((Number) actualValue).doubleValue(),
                        closeTo(((Number) expectedValue).doubleValue(), epsilon)
                    );
                } else {
                    assertThat(
                        "field [" + getField() + "] doesn't match the expected value",
                        ((Number) actualValue).doubleValue(),
                        equalTo(((Number) expectedValue).doubleValue())
                    );
                }
                return;
            }
        }

        if (epsilon != null && actualValue instanceof Number && expectedValue instanceof Number) {
            assertThat(
                "field [" + getField() + "] doesn't match the expected value within epsilon " + epsilon,
                ((Number) actualValue).doubleValue(),
                closeTo(((Number) expectedValue).doubleValue(), epsilon)
            );
            return;
        }

        if (expectedValue.equals(actualValue) == false) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compare(getField(), true, actualValue, expectedValue);
            throw new AssertionError(getField() + " didn't match expected value:\n" + message);
        }
    }
}

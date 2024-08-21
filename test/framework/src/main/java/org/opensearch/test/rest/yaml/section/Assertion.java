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

import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.test.rest.yaml.ClientYamlTestExecutionContext;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * Base class for executable sections that hold assertions
 */
public abstract class Assertion implements ExecutableSection {
    private final XContentLocation location;
    private final String field;
    private final Object expectedValue;

    protected Assertion(XContentLocation location, String field, Object expectedValue) {
        this.location = location;
        this.field = field;
        this.expectedValue = expectedValue;
    }

    public final String getField() {
        return field;
    }

    public final Object getExpectedValue() {
        return expectedValue;
    }

    protected final Object resolveExpectedValue(ClientYamlTestExecutionContext executionContext) throws IOException {
        if (expectedValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) expectedValue;
            return executionContext.stash().replaceStashedValues(map);
        }

        if (executionContext.stash().containsStashedValue(expectedValue)) {
            return executionContext.stash().getValue(expectedValue.toString());
        }
        return expectedValue;
    }

    protected final Object getActualValue(ClientYamlTestExecutionContext executionContext) throws IOException {
        if (executionContext.stash().containsStashedValue(field)) {
            return executionContext.stash().getValue(field);
        }
        return executionContext.response(field);
    }

    static Object convertActualValue(Object actualValue, Object expectedValue) {
        if (actualValue == null || expectedValue.getClass().isAssignableFrom(actualValue.getClass())) {
            return actualValue;
        }
        if (actualValue instanceof Number && expectedValue instanceof Number) {
            if (expectedValue instanceof Float) {
                return Float.parseFloat(actualValue.toString());
            } else if (expectedValue instanceof Double) {
                return Double.parseDouble(actualValue.toString());
            } else if (expectedValue instanceof Integer) {
                return Integer.parseInt(actualValue.toString());
            } else if (expectedValue instanceof Long) {
                return Long.parseLong(actualValue.toString());
            }
        }
        // Force a class cast exception here, so developers can flesh out the above logic as needed.
        try {
            expectedValue.getClass().cast(actualValue);
        } catch (ClassCastException e) {
            fail(
                "Type mismatch: Expected value ("
                    + expectedValue
                    + ") has type "
                    + expectedValue.getClass()
                    + ". "
                    + "Actual value ("
                    + actualValue
                    + ") has type "
                    + actualValue.getClass()
                    + "."
            );
        }
        return actualValue;
    }

    @Override
    public XContentLocation getLocation() {
        return location;
    }

    @Override
    public final void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
        doAssert(getActualValue(executionContext), resolveExpectedValue(executionContext));
    }

    /**
     * Executes the assertion comparing the actual value (parsed from the response) with the expected one
     */
    protected abstract void doAssert(Object actualValue, Object expectedValue);

    /**
     * a utility to get the class of an object, protecting for null (i.e., returning null if the input is null)
     */
    protected Class<?> safeClass(Object o) {
        return o == null ? null : o.getClass();
    }
}

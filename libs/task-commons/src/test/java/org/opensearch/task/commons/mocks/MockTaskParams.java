/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.task.commons.mocks;

import org.opensearch.task.commons.task.TaskParams;

public class MockTaskParams extends TaskParams {

    private final String value;

    public MockTaskParams(String mockValue) {
        super();
        value = mockValue;
    }

    public String getValue() {
        return value;
    }
}

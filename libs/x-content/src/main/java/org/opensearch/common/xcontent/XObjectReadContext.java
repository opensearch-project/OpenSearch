/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import tools.jackson.core.ObjectReadContext;

public class XObjectReadContext extends ObjectReadContext.Base {
    private static final XObjectReadContext DEFAULT_INSTANCE = new XObjectReadContext();

    public static XObjectReadContext create() {
        return DEFAULT_INSTANCE;
    }

    private XObjectReadContext() {}
}

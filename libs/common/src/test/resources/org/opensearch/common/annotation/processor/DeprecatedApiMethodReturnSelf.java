/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.annotation.processor;

import org.opensearch.common.annotation.DeprecatedApi;

@DeprecatedApi(since = "1.0.0")
public class DeprecatedApiMethodReturnSelf {
    public DeprecatedApiMethodReturnSelf method() {
        return new DeprecatedApiMethodReturnSelf();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.wlm.MutableQueryGroupFragment;

public class QueryGroupActionTestUtils {
    public static UpdateQueryGroupRequest updateQueryGroupRequest(String name, MutableQueryGroupFragment mutableQueryGroupFragment) {
        return new UpdateQueryGroupRequest(name, mutableQueryGroupFragment);
    }
}

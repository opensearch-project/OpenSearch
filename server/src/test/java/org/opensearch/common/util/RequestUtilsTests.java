/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.core.common.Strings;
import org.opensearch.test.OpenSearchTestCase;

public class RequestUtilsTests extends OpenSearchTestCase {

    public void testGenerateID() {
        assertTrue(Strings.hasText(RequestUtils.generateID()));
    }
}

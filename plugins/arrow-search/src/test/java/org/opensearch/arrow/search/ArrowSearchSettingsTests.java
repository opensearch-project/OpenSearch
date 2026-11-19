/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowSearchSettingsTests extends OpenSearchTestCase {

    public void testClientIdleTimeoutIsAtLeastASecond() {
        // A zero timeout would fail every search whose call is not ready for a moment after queueing a batch.
        for (String value : new String[] { "0s", "0", "999ms" }) {
            Settings settings = Settings.builder().put(ArrowSearchSettings.CLIENT_IDLE_TIMEOUT.getKey(), value).build();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ArrowSearchSettings.CLIENT_IDLE_TIMEOUT.get(settings)
            );
            assertTrue(e.getMessage(), e.getMessage().contains("must be >= [1s]"));
        }
        Settings oneSecond = Settings.builder().put(ArrowSearchSettings.CLIENT_IDLE_TIMEOUT.getKey(), "1s").build();
        assertEquals(TimeValue.timeValueSeconds(1), ArrowSearchSettings.CLIENT_IDLE_TIMEOUT.get(oneSecond));
        assertEquals(TimeValue.timeValueSeconds(60), ArrowSearchSettings.CLIENT_IDLE_TIMEOUT.get(Settings.EMPTY));
    }
}

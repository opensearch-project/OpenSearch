/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Contract test: DataFusion must declare a scan capability over {@link FieldType#NESTED}. Without it,
 * {@code OpenSearchTableScanRule} finds no viable backend for a nested-field projection (e.g.
 * {@code | fields events}) and rejects the plan.
 */
public class NestedScanCapabilityTests extends OpenSearchTestCase {

    public void testNestedIsScanCapable() {
        BackendCapabilityProvider provider = new DataFusionAnalyticsBackendPlugin(new DataFusionPlugin()).getCapabilityProvider();
        boolean nestedScannable = provider.scanCapabilities()
            .stream()
            .anyMatch(cap -> cap.supportedFieldTypes().contains(FieldType.NESTED));
        assertTrue("DataFusion must declare a scan capability for NESTED", nestedScannable);
    }
}

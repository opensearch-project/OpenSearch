/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.enums;

import org.opensearch.test.OpenSearchTestCase;

public class AdmissionControlModeTests extends OpenSearchTestCase {

    public void testValidActionType() {
        assertEquals(AdmissionControlMode.DISABLED.getMode(), "disabled");
        assertEquals(AdmissionControlMode.ENFORCED.getMode(), "enforced");
        assertEquals(AdmissionControlMode.MONITOR.getMode(), "monitor_only");
        assertEquals(AdmissionControlMode.fromName("disabled"), AdmissionControlMode.DISABLED);
        assertEquals(AdmissionControlMode.fromName("enforced"), AdmissionControlMode.ENFORCED);
        assertEquals(AdmissionControlMode.fromName("monitor_only"), AdmissionControlMode.MONITOR);
    }

    public void testInValidActionType() {
        String name = "TEST";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> AdmissionControlMode.fromName(name));
        assertEquals(ex.getMessage(), "Invalid AdmissionControlMode: " + name);
    }
}

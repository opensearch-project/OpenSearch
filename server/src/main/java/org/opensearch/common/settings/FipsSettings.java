/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.common.settings.Setting.Property;

/**
 * Settings used for NIST FIPS 140-2 compliance
 */
public class FipsSettings {

    public static final Setting<Boolean> FIPS_ENABLED = Setting.boolSetting("fips.approved", false, Property.NodeScope);

}

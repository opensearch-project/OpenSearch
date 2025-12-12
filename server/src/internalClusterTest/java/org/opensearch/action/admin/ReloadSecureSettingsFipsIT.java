/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin;

public class ReloadSecureSettingsFipsIT extends ReloadSecureSettingsIT {

    public void testMisbehavingPlugin() throws Exception {
        assumeFalse("Can't use empty password in a FIPS JVM", inFipsJvm());
    }

    public void testReloadWhileKeystoreChanged() throws Exception {
        assumeFalse("Can't use empty password in a FIPS JVM", inFipsJvm());
    }
}

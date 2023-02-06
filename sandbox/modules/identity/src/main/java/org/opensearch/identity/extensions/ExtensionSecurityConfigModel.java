/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.extensions;

public abstract class ExtensionSecurityConfigModel {

    public abstract ExtensionSecurityConfig getExtensionSecurityConfig(String extensionId);

    public abstract boolean exists(String extensionId);

}

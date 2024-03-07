/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

// Type of token credentials that the plugin supports
public enum TokenCredentialType {
    MANAGED_IDENTITY,
    NOT_APPLICABLE
}

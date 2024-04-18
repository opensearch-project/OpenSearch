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
    // This represents the support for ManagedIdentityCredential
    MANAGED_IDENTITY,
    // This is the default when token credential is not configure.
    // SAS token or Account Key will be used for authentication instead.
    NOT_APPLICABLE
}

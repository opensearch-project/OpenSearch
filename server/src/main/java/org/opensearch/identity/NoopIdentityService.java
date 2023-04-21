/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

/**
 * Placeholder created by node in case of extensions feature flag set to false. See {@link org.opensearch.common.util.FeatureFlags}
 */
public class NoopIdentityService extends IdentityService {

    public NoopIdentityService() {
        super();
    }
}

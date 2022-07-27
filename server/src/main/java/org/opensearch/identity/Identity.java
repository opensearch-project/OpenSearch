/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.identity;

/**
 * The distinguishing character or personality of an entity within OpenSearch.
 */
public interface Identity {

    /** A durable, reusable identifier of this identity */
 
    String getId();
}

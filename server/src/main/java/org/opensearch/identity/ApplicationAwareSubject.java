/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

/**
 * This interface extends the ScopeAwareSubject but expects implementing classes to be able to verify whether an associated
 * application exists.
 */
public interface ApplicationAwareSubject extends ScopeAwareSubject {

    boolean applicationExists();

}

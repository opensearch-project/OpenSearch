/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import java.util.Set;
import org.opensearch.identity.scopes.Scope;

/**
 * This interface extends the ScopeAwareSubject but expects implementing classes to be able to verify whether an associated
 * application exists.
 *
 * It is separate from ScopeAwareSubject since traditional Users would not have an Application associated with their Subjects, but could still
 * make use of Scopes.
 */
public interface ApplicationAwareSubject extends Subject {

    Set<Scope> getScopes();

    boolean applicationExists();
}

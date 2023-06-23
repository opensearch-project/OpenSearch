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
 * This interface defines a ScopeAwareSubject.
 *
 * A ScopeAwawreSubject is an extension of the Subject interface which adds the concept of scopes.
 * Specifically, a ScopeAwareSubject must be able to get its own scopes (as a set of Strings) and set its own scopes,
 *
 */
public interface ScopeAwareSubject extends Subject {

    Set<String> getScopes();

    void setScopes(Set<Scope> scopes);
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PrivilegesEvaluatorResponse {
    boolean allowed = false;
    Set<String> missingPrivileges = new HashSet<String>();
    Set<String> missingSecurityRoles = new HashSet<>();
    Set<String> resolvedSecurityRoles = new HashSet<>();
    Map<String, Set<String>> allowedFlsFields;
    Map<String, Set<String>> maskedFields;
    Map<String, Set<String>> queries;
    PrivilegesEvaluatorResponseState state = PrivilegesEvaluatorResponseState.PENDING;
    CreateIndexRequestBuilder createIndexRequestBuilder;

    public boolean isAllowed() {
        return allowed;
    }

    public Set<String> getMissingPrivileges() {
        return new HashSet<String>(missingPrivileges);
    }

    public Set<String> getMissingSecurityRoles() {
        return new HashSet<>(missingSecurityRoles);
    }

    public Set<String> getResolvedSecurityRoles() {
        return new HashSet<>(resolvedSecurityRoles);
    }

    public Map<String, Set<String>> getAllowedFlsFields() {
        return allowedFlsFields;
    }

    public Map<String, Set<String>> getMaskedFields() {
        return maskedFields;
    }

    public Map<String, Set<String>> getQueries() {
        return queries;
    }

    public CreateIndexRequestBuilder getCreateIndexRequestBuilder() {
        return createIndexRequestBuilder;
    }

    public PrivilegesEvaluatorResponse markComplete() {
        this.state = PrivilegesEvaluatorResponseState.COMPLETE;
        return this;
    }

    public PrivilegesEvaluatorResponse markPending() {
        this.state = PrivilegesEvaluatorResponseState.PENDING;
        return this;
    }

    public boolean isComplete() {
        return this.state == PrivilegesEvaluatorResponseState.COMPLETE;
    }

    public boolean isPending() {
        return this.state == PrivilegesEvaluatorResponseState.PENDING;
    }

    @Override
    public String toString() {
        return "PrivEvalResponse [allowed="
            + allowed
            + ", missingPrivileges="
            + missingPrivileges
            + ", allowedFlsFields="
            + allowedFlsFields
            + ", maskedFields="
            + maskedFields
            + ", queries="
            + queries
            + "]";
    }

    public static enum PrivilegesEvaluatorResponseState {
        PENDING,
        COMPLETE;
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.index.fielddomain.FieldDomain;

import java.util.List;
import java.util.Objects;

/**
 * Ordered collection of field-domain evaluators.
 *
 * The collection keeps pruning conservative by treating "no evaluator can prune" as "the index may match".
 */
public final class FieldDomainEvaluators {

    private static final FieldDomainEvaluators DEFAULT = new FieldDomainEvaluators(List.of(new DateRangeFieldDomainEvaluator()));

    private final List<FieldDomainEvaluator> evaluators;

    /**
     * Creates an evaluator collection.
     *
     * @param evaluators evaluators to run for each field-domain/constraint pair
     */
    public FieldDomainEvaluators(List<FieldDomainEvaluator> evaluators) {
        Objects.requireNonNull(evaluators, "evaluators must not be null");
        this.evaluators = List.copyOf(evaluators);
    }

    /**
     * Returns the built-in evaluator collection.
     */
    public static FieldDomainEvaluators defaultEvaluators() {
        return DEFAULT;
    }

    /**
     * Returns whether an index with the supplied bounds may match the supplied query constraint.
     *
     * @return {@code false} if any evaluator proves the pair is disjoint; otherwise {@code true}
     */
    public boolean canMatch(FieldDomain domain, QueryConstraint constraint, FieldDomainEvaluationContext context) {
        Objects.requireNonNull(domain, "domain must not be null");
        Objects.requireNonNull(constraint, "constraint must not be null");
        Objects.requireNonNull(context, "context must not be null");

        if (domain.finalized() == false || domain.field().equals(constraint.field()) == false) {
            return true;
        }

        for (FieldDomainEvaluator evaluator : evaluators) {
            if (evaluator.canMatch(domain, constraint, context) == false) {
                return false;
            }
        }

        return true;
    }
}

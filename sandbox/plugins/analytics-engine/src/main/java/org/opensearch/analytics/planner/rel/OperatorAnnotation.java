/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import java.util.List;

/**
 * Common interface for all annotation types (predicate, aggregate call,
 * project expression). Allows PlanForker to work generically without
 * knowing the specific annotation type.
 *
 * @opensearch.internal
 */
public interface OperatorAnnotation {

    int getAnnotationId();

    List<String> getViableBackends();

    /** Returns a copy of this annotation with viableBackends narrowed to the given backend. */
    OperatorAnnotation narrowTo(String backend);
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.search.Scorer;
import org.opensearch.common.annotation.PublicApi;

/**
 * Interface for scorer wrappers that decorate a {@link Scorer} with additional behavior
 * (e.g., profiling instrumentation) while preserving access to the original scorer.
 * <p>
 * When search profiling is enabled, OpenSearch wraps scorers in profiling decorators that
 * are package-private. This interface provides a public contract for plugins to detect
 * and unwrap such wrappers, accessing the underlying scorer without requiring reflection
 * or knowledge of package-private implementation classes.
 * <p>
 * Example usage in a plugin:
 * <pre>{@code
 * if (scorer instanceof WrappedScorerAccessor) {
 *     Scorer wrappedScorer = ((WrappedScorerAccessor) scorer).getWrappedScorer();
 *     // access custom scorer methods
 * }
 * }</pre>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public interface WrappedScorerAccessor {

    /**
     * Returns the underlying wrapped scorer.
     *
     * @return the original scorer that was wrapped
     */
    Scorer getWrappedScorer();
}

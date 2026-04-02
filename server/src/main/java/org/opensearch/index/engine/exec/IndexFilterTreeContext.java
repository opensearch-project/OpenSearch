/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.IndexFilterTree;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Holds per-leaf {@link IndexFilterContext} instances for a tree query.
 * <p>
 * One context per FTS leaf in the {@link IndexFilterTree}. Each leaf
 * context manages its own collectors independently.
 *
 * @param <C> the per-leaf context type
 * @opensearch.experimental
 */
@ExperimentalApi
public class IndexFilterTreeContext<C extends IndexFilterContext> implements Closeable {

    private final IndexFilterTree tree;
    private final List<C> leafContexts;

    /**
     * Creates a tree context with the given per-leaf contexts.
     *
     * @param tree the boolean filter tree
     * @param leafContexts one context per index leaf
     */
    public IndexFilterTreeContext(IndexFilterTree tree, List<C> leafContexts) {
        this.tree = tree;
        this.leafContexts = Collections.unmodifiableList(new ArrayList<>(leafContexts));
    }

    /** Returns the boolean filter tree. */
    public IndexFilterTree tree() {
        return tree;
    }

    /** Returns the context for the given index leaf. */
    public C leafContext(int leafIndex) {
        return leafContexts.get(leafIndex);
    }

    /** Returns the number of index leaf contexts. */
    public int leafCount() {
        return leafContexts.size();
    }

    /** Returns all leaf contexts. */
    public List<C> leafContexts() {
        return leafContexts;
    }

    @Override
    public void close() throws IOException {
        IOException firstException = null;
        for (C ctx : leafContexts) {
            try {
                ctx.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }
}

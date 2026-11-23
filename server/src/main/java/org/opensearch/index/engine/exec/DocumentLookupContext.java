/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.action.support.DocumentLevelReadAccessPolicy;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;
import org.opensearch.index.engine.Engine;

import java.util.Objects;

/** Request-scoped dependencies for a pluggable document lookup. */
@ExperimentalApi
public final class DocumentLookupContext {

    private final Engine.Get get;
    private final Index index;
    private final DocumentLevelReadAccessPolicy documentLevelReadAccessPolicy;

    private DocumentLookupContext(Engine.Get get, Index index, DocumentLevelReadAccessPolicy documentLevelReadAccessPolicy) {
        this.get = Objects.requireNonNull(get, "get must not be null");
        this.index = Objects.requireNonNull(index, "index must not be null");
        this.documentLevelReadAccessPolicy = Objects.requireNonNull(
            documentLevelReadAccessPolicy,
            "documentLevelReadAccessPolicy must not be null"
        );
    }

    public static DocumentLookupContext of(Engine.Get get, Index index, DocumentLevelReadAccessPolicy documentLevelReadAccessPolicy) {
        return new DocumentLookupContext(get, index, documentLevelReadAccessPolicy);
    }

    public Engine.Get get() {
        return get;
    }

    public Index index() {
        return index;
    }

    public DocumentLevelReadAccessPolicy documentLevelReadAccessPolicy() {
        return documentLevelReadAccessPolicy;
    }
}

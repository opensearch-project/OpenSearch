/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.opensearch.common.Nullable;

/**
 * Returns the terms of a {@link TermsEnum} that start with a prefix, in byte order. It seeks straight to
 * the prefix, or to {@code searchAfter} when that sorts later, and ends at the first term past the prefix.
 * The {@code searchAfter} term itself is skipped.
 *
 * @opensearch.internal
 */
final class PrefixTermsEnum extends FilteredTermsEnum {

    private final BytesRef prefix;
    @Nullable
    private final BytesRef searchAfter;

    PrefixTermsEnum(TermsEnum in, BytesRef prefix, @Nullable BytesRef searchAfter) {
        super(in);
        this.prefix = prefix;
        this.searchAfter = searchAfter;
        setInitialSeekTerm(searchAfter != null && searchAfter.compareTo(prefix) > 0 ? searchAfter : prefix);
    }

    @Override
    protected AcceptStatus accept(BytesRef term) {
        if (StringHelper.startsWith(term, prefix) == false) {
            return AcceptStatus.END;
        }
        if (searchAfter != null && term.bytesEquals(searchAfter)) {
            return AcceptStatus.NO;
        }
        return AcceptStatus.YES;
    }
}

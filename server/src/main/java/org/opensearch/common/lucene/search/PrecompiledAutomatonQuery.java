/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;

/**
 * A {@link MultiTermQuery} backed by a pre-built {@link CompiledAutomaton}.
 *
 * <p>This is functionally equivalent to Lucene's {@code AutomatonQuery} but accepts an
 * already-compiled automaton, avoiding the expensive determinization and UTF32-to-UTF8
 * compilation that {@code AutomatonQuery}'s constructor performs unconditionally.
 *
 * @opensearch.internal
 */
public final class PrecompiledAutomatonQuery extends MultiTermQuery implements Accountable {
    private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(PrecompiledAutomatonQuery.class);

    private final Term term;
    private final CompiledAutomaton compiled;
    private final String pattern;
    private final long ramBytesUsed;

    public PrecompiledAutomatonQuery(Term term, CompiledAutomaton compiled, String pattern) {
        this(term, compiled, pattern, CONSTANT_SCORE_BLENDED_REWRITE);
    }

    public PrecompiledAutomatonQuery(Term term, CompiledAutomaton compiled, String pattern, RewriteMethod rewriteMethod) {
        super(term.field(), rewriteMethod);
        this.term = term;
        this.compiled = compiled;
        this.pattern = pattern;
        this.ramBytesUsed = BASE_RAM_BYTES + term.ramBytesUsed() + compiled.ramBytesUsed();
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        return compiled.getTermsEnum(terms);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            compiled.visit(visitor, this, field);
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append("/");
        buffer.append(pattern);
        buffer.append("/");
        return buffer.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + compiled.hashCode();
        result = prime * result + term.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        PrecompiledAutomatonQuery other = (PrecompiledAutomatonQuery) obj;
        return compiled.equals(other.compiled) && term.equals(other.term);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }
}

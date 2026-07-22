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
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.opensearch.common.annotation.InternalApi;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link MultiTermQuery} implementation that wraps a pre-compiled {@link CompiledAutomaton},
 * avoiding the performance overhead of recompiling the automaton on every query construction.
 *
 * @opensearch.internal
 */
@InternalApi
public class PrecompiledAutomatonQuery extends MultiTermQuery {

    private final Term term;
    private final CompiledAutomaton compiled;

    public PrecompiledAutomatonQuery(Term term, CompiledAutomaton compiledAutomaton, RewriteMethod rewriteMethod) {
        super(term.field(), rewriteMethod);
        this.term = Objects.requireNonNull(term, "term cannot be null");
        this.compiled = Objects.requireNonNull(compiledAutomaton, "compiledAutomaton cannot be null");
    }

    public PrecompiledAutomatonQuery(Term term, CompiledAutomaton compiledAutomaton) {
        this(term, compiledAutomaton, CONSTANT_SCORE_BLENDED_REWRITE);
    }

    public Term getTerm() {
        return term;
    }

    public CompiledAutomaton getCompiledAutomaton() {
        return compiled;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        return compiled.getTermsEnum(terms);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(getField())) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append(getClass().getSimpleName());
        buffer.append(": ");
        buffer.append(term.text());
        return buffer.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        PrecompiledAutomatonQuery other = (PrecompiledAutomatonQuery) obj;
        return Objects.equals(term, other.term) && Objects.equals(compiled, other.compiled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term, compiled);
    }
}

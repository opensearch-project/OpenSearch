/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.StringLength;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.be.lucene.predicate.LikePredicateHandler;
import org.opensearch.be.lucene.predicate.RexToQueryBuilderConverter;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link RexToQueryBuilderConverter}.
 */
class RexToQueryBuilderConverterPropertyTests {

    private final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    /**
     * Builds a single-column VARCHAR row type with the given field name.
     */
    private RelDataType buildRowType(String fieldName) {
        return typeFactory.builder()
            .add(fieldName, typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .build();
    }

    /**
     * Builds an EQUALS RexCall: field(index=0) = 'value'.
     */
    private RexNode buildEqualsCall(String value) {
        RexNode fieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode literal = rexBuilder.makeLiteral(value);
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal);
    }
    /**
     * Generates ASCII-only string values (printable range 0x20–0x7E) to stay within
     * Calcite's ISO-8859-1 character set constraint for RexBuilder.makeLiteral.
     */
    @Provide
    Arbitrary<String> asciiValues() {
        return Arbitraries.strings().ascii().ofMinLength(0).ofMaxLength(100);
    }


    @Property(tries = 100)
    // Feature: lucene-backend-plugin, Property 1: Text equality produces TermQueryBuilder
    void textEqualityProducesTermQueryBuilder(
        @ForAll @AlphaChars @StringLength(min = 1, max = 50) String fieldName,
        @ForAll("asciiValues") String value
    ) {
        RelDataType rowType = buildRowType(fieldName);
        RexNode equalsCall = buildEqualsCall(value);

        RexToQueryBuilderConverter converter = new RexToQueryBuilderConverter(rowType);
        QueryBuilder result = converter.convert(equalsCall);

        assertThat(result).isInstanceOf(TermQueryBuilder.class);
        TermQueryBuilder termQuery = (TermQueryBuilder) result;
        assertThat(termQuery.fieldName()).isEqualTo(fieldName);
        assertThat(termQuery.value()).isEqualTo(value);
    }

    /**
     * Builds a LIKE RexCall: field(index=0) LIKE 'pattern'.
     */
    private RexNode buildLikeCall(String pattern) {
        RexNode fieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode literal = rexBuilder.makeLiteral(pattern);
        return rexBuilder.makeCall(SqlStdOperatorTable.LIKE, fieldRef, literal);
    }

    /**
     * Generates a LIKE pattern that ends with '%' and has no other wildcards (trailing-percent-only).
     * The body is 0–50 alphanumeric characters followed by a single '%'.
     */
    @Provide
    Arbitrary<String> trailingPercentPatterns() {
        return Arbitraries.strings().alpha().numeric().ofMinLength(0).ofMaxLength(50)
            .map(body -> body + "%");
    }

    /**
     * Generates a LIKE pattern that contains '%' or '_' in non-trailing positions,
     * ensuring it does NOT qualify as trailing-percent-only.
     */
    @Provide
    Arbitrary<String> generalWildcardPatterns() {
        // Patterns like: "%abc", "a%b", "a_b", "_abc%", "%a%b"
        return Arbitraries.of("%", "_").flatMap(wildcard ->
            Arbitraries.strings().alpha().numeric().ofMinLength(1).ofMaxLength(20).flatMap(prefix ->
                Arbitraries.strings().alpha().numeric().ofMinLength(1).ofMaxLength(20).map(suffix ->
                    prefix + wildcard + suffix
                )
            )
        );
    }

    @Property(tries = 100)
    // Feature: lucene-backend-plugin, Property 2: LIKE trailing-percent-only produces PrefixQueryBuilder
    void likeTrailingPercentProducesPrefixQueryBuilder(
        @ForAll @AlphaChars @StringLength(min = 1, max = 50) String fieldName,
        @ForAll("trailingPercentPatterns") String pattern
    ) {
        RelDataType rowType = buildRowType(fieldName);
        RexNode likeCall = buildLikeCall(pattern);

        RexToQueryBuilderConverter converter = new RexToQueryBuilderConverter(rowType);
        QueryBuilder result = converter.convert(likeCall);

        assertThat(result).isInstanceOf(PrefixQueryBuilder.class);
        PrefixQueryBuilder prefixQuery = (PrefixQueryBuilder) result;
        assertThat(prefixQuery.fieldName()).isEqualTo(fieldName);
        // The prefix is the pattern without the trailing '%'
        String expectedPrefix = pattern.substring(0, pattern.length() - 1);
        assertThat(prefixQuery.value()).isEqualTo(expectedPrefix);
    }

    @Property(tries = 100)
    // Feature: lucene-backend-plugin, Property 2: LIKE general wildcard produces WildcardQueryBuilder
    void likeGeneralWildcardProducesWildcardQueryBuilder(
        @ForAll @AlphaChars @StringLength(min = 1, max = 50) String fieldName,
        @ForAll("generalWildcardPatterns") String pattern
    ) {
        RelDataType rowType = buildRowType(fieldName);
        RexNode likeCall = buildLikeCall(pattern);

        RexToQueryBuilderConverter converter = new RexToQueryBuilderConverter(rowType);
        QueryBuilder result = converter.convert(likeCall);

        assertThat(result).isInstanceOf(WildcardQueryBuilder.class);
        WildcardQueryBuilder wildcardQuery = (WildcardQueryBuilder) result;
        assertThat(wildcardQuery.fieldName()).isEqualTo(fieldName);
        // Verify SQL wildcards are translated: '%' → '*', '_' → '?'
        String expectedLucenePattern = LikePredicateHandler.translateSqlWildcards(pattern);
        assertThat(wildcardQuery.value()).isEqualTo(expectedLucenePattern);
    }
}

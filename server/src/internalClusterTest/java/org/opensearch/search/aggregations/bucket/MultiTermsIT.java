/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.bucket.terms.BaseStringTermsTestCase;
import org.opensearch.search.aggregations.bucket.terms.StringTermsIT;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collections;

import static java.util.Arrays.asList;
import static org.opensearch.search.aggregations.AggregationBuilders.multiTerms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * Extend {@link BaseStringTermsTestCase}.
 */
@OpenSearchIntegTestCase.SuiteScopeTestCase
public class MultiTermsIT extends BaseStringTermsTestCase {

    public MultiTermsIT(Settings staticSettings) {
        super(staticSettings);
    }

    // the main purpose of this test is to make sure we're not allocating 2GB of memory per shard
    public void testSizeIsZero() {
        final int minDocCount = randomInt(1);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("high_card_idx")
                .addAggregation(
                    multiTerms("mterms").terms(
                        asList(
                            new MultiTermsValuesSourceConfig.Builder().setFieldName(SINGLE_VALUED_FIELD_NAME).build(),
                            new MultiTermsValuesSourceConfig.Builder().setFieldName(MULTI_VALUED_FIELD_NAME).build()
                        )
                    ).minDocCount(minDocCount).size(0)
                )
                .get()
        );
        assertThat(exception.getMessage(), containsString("[size] must be greater than 0. Found [0] in [mterms]"));
    }

    public void testSingleValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                multiTerms("mterms").terms(
                    asList(
                        new MultiTermsValuesSourceConfig.Builder().setFieldName("i").build(),
                        new MultiTermsValuesSourceConfig.Builder().setFieldName(SINGLE_VALUED_FIELD_NAME)
                            .setScript(
                                new Script(
                                    ScriptType.INLINE,
                                    StringTermsIT.CustomScriptPlugin.NAME,
                                    "'foo_' + _value",
                                    Collections.emptyMap()
                                )
                            )
                            .build()
                    )
                )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("mterms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("mterms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey(i + "|foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(i + "|foo_val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testSingleValuedFieldWithScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                multiTerms("mterms").terms(
                    asList(
                        new MultiTermsValuesSourceConfig.Builder().setFieldName("i").build(),
                        new MultiTermsValuesSourceConfig.Builder().setScript(
                            new Script(
                                ScriptType.INLINE,
                                StringTermsIT.CustomScriptPlugin.NAME,
                                "doc['" + SINGLE_VALUED_FIELD_NAME + "'].value",
                                Collections.emptyMap()
                            )
                        ).setUserValueTypeHint(ValueType.STRING).build()
                    )
                )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("mterms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("mterms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey(i + "|val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(i + "|val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testMultiValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                multiTerms("mterms").terms(
                    asList(
                        new MultiTermsValuesSourceConfig.Builder().setFieldName("tag").build(),
                        new MultiTermsValuesSourceConfig.Builder().setFieldName(MULTI_VALUED_FIELD_NAME)
                            .setScript(
                                new Script(
                                    ScriptType.INLINE,
                                    StringTermsIT.CustomScriptPlugin.NAME,
                                    "_value.substring(0,3)",
                                    Collections.emptyMap()
                                )
                            )
                            .build()
                    )
                )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("mterms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("mterms"));
        assertThat(terms.getBuckets().size(), equalTo(2));

        Terms.Bucket bucket = terms.getBucketByKey("more|val");
        assertThat(bucket, notNullValue());
        assertThat(key(bucket), equalTo("more|val"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = terms.getBucketByKey("less|val");
        assertThat(bucket, notNullValue());
        assertThat(key(bucket), equalTo("less|val"));
        assertThat(bucket.getDocCount(), equalTo(2L));
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BaseAggregationTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValueType;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class MultiTermsAggregationBuilderTests extends BaseAggregationTestCase<MultiTermsAggregationBuilder> {

    @Override
    protected MultiTermsAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        MultiTermsAggregationBuilder factory = new MultiTermsAggregationBuilder(name);

        int termsCount = randomIntBetween(2, 10);
        List<MultiTermsValuesSourceConfig> fieldConfigs = new ArrayList<>();
        for (int i = 0; i < termsCount; i++) {
            fieldConfigs.add(randomFieldConfig());
        }
        factory.terms(fieldConfigs);

        if (randomBoolean()) {
            factory.size(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            int minDocCount = randomInt(4);
            switch (minDocCount) {
                case 0:
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                    minDocCount = randomIntBetween(0, Integer.MAX_VALUE);
                    break;
                default:
                    fail();
            }
            factory.minDocCount(minDocCount);
        }
        if (randomBoolean()) {
            int shardMinDocCount = randomInt(4);
            switch (shardMinDocCount) {
                case 0:
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                    shardMinDocCount = randomIntBetween(0, Integer.MAX_VALUE);
                    break;
                default:
                    fail();
            }
            factory.shardMinDocCount(shardMinDocCount);
        }
        if (randomBoolean()) {
            factory.collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()));
        }
        if (randomBoolean()) {
            List<BucketOrder> order = randomOrder();
            if (order.size() == 1 && randomBoolean()) {
                factory.order(order.get(0));
            } else {
                factory.order(order);
            }
        }
        if (randomBoolean()) {
            factory.showTermDocCountError(randomBoolean());
        }
        return factory;
    }

    public void testInvalidTermsParams() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new MultiTermsAggregationBuilder("_name").terms(Collections.singletonList(randomFieldConfig()));
        });
        assertEquals(
            "multi term aggregation must has at least 2 terms. Found [1] in [_name] Use terms aggregation for single term aggregation",
            exception.getMessage()
        );

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> { new MultiTermsAggregationBuilder("_name").terms(Collections.emptyList()); }
        );
        assertEquals("multi term aggregation must has at least 2 terms. Found [0] in [_name]", exception.getMessage());

        exception = expectThrows(IllegalArgumentException.class, () -> { new MultiTermsAggregationBuilder("_name").terms(null); });
        assertEquals("[terms] must not be null. Found null terms in [_name]", exception.getMessage());
    }

    private List<BucketOrder> randomOrder() {
        List<BucketOrder> orders = new ArrayList<>();
        switch (randomInt(4)) {
            case 0:
                orders.add(BucketOrder.key(randomBoolean()));
                break;
            case 1:
                orders.add(BucketOrder.count(randomBoolean()));
                break;
            case 2:
                orders.add(BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomBoolean()));
                break;
            case 3:
                orders.add(BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomAlphaOfLengthBetween(3, 20), randomBoolean()));
                break;
            case 4:
                int numOrders = randomIntBetween(1, 3);
                for (int i = 0; i < numOrders; i++) {
                    orders.addAll(randomOrder());
                }
                break;
            default:
                fail();
        }
        return orders;
    }

    protected static MultiTermsValuesSourceConfig randomFieldConfig() {
        String field = randomAlphaOfLength(10);
        Object missing = randomBoolean() ? randomAlphaOfLength(10) : null;
        ZoneId timeZone = randomBoolean() ? randomZone() : null;
        ValueType userValueTypeHint = randomBoolean()
            ? randomFrom(ValueType.STRING, ValueType.LONG, ValueType.DOUBLE, ValueType.DATE, ValueType.IP)
            : null;
        String format = randomBoolean() ? randomNumericDocValueFormat().toString() : null;
        return randomFieldOrScript(
            new MultiTermsValuesSourceConfig.Builder().setMissing(missing)
                .setTimeZone(timeZone)
                .setUserValueTypeHint(userValueTypeHint)
                .setFormat(format),
            field
        ).build();
    }

    protected static MultiTermsValuesSourceConfig.Builder randomFieldOrScript(MultiTermsValuesSourceConfig.Builder builder, String field) {
        int choice = randomInt(1);
        switch (choice) {
            case 0:
                builder.setFieldName(field);
                break;
            case 1:
                builder.setScript(mockScript("doc[" + field + "] + 1"));
                break;
            default:
                throw new AssertionError("Unknown random operation [" + choice + "]");
        }
        return builder;
    }

    /**
     * @return a random {@link DocValueFormat} that can be used in aggregations which
     * compute numbers.
     */
    protected static DocValueFormat randomNumericDocValueFormat() {
        final List<Supplier<DocValueFormat>> formats = new ArrayList<>(3);
        formats.add(() -> DocValueFormat.RAW);
        formats.add(() -> new DocValueFormat.Decimal(randomFrom("###.##", "###,###.##")));
        return randomFrom(formats).get();
    }
}

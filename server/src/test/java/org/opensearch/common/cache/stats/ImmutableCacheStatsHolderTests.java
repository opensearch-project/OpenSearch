/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class ImmutableCacheStatsHolderTests extends OpenSearchTestCase {
    private final String storeName = "dummy_store";

    public void testSerialization() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
        String[] levels = dimensionNames.toArray(new String[0]);
        DefaultCacheStatsHolder statsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(statsHolder, 10);
        DefaultCacheStatsHolderTests.populateStats(statsHolder, usedDimensionValues, 100, 10);
        ImmutableCacheStatsHolder stats = statsHolder.getImmutableCacheStatsHolder(levels);
        assertNotEquals(0, stats.getStatsRoot().children.size());

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        ImmutableCacheStatsHolder deserialized = new ImmutableCacheStatsHolder(is);

        assertEquals(stats, deserialized);

        // also test empty dimension stats
        ImmutableCacheStatsHolder emptyDims = statsHolder.getImmutableCacheStatsHolder(new String[] {});
        assertEquals(0, emptyDims.getStatsRoot().children.size());
        assertEquals(stats.getTotalStats(), emptyDims.getTotalStats());

        os = new BytesStreamOutput();
        emptyDims.writeTo(os);
        is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        deserialized = new ImmutableCacheStatsHolder(is);

        assertEquals(emptyDims, deserialized);
    }

    public void testEquals() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
        String[] levels = dimensionNames.toArray(new String[0]);
        DefaultCacheStatsHolder statsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        DefaultCacheStatsHolder differentStoreNameStatsHolder = new DefaultCacheStatsHolder(dimensionNames, "nonMatchingStoreName");
        DefaultCacheStatsHolder nonMatchingStatsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(statsHolder, 10);
        DefaultCacheStatsHolderTests.populateStats(List.of(statsHolder, differentStoreNameStatsHolder), usedDimensionValues, 100, 10);
        DefaultCacheStatsHolderTests.populateStats(nonMatchingStatsHolder, usedDimensionValues, 100, 10);
        ImmutableCacheStatsHolder stats = statsHolder.getImmutableCacheStatsHolder(levels);

        ImmutableCacheStatsHolder secondStats = statsHolder.getImmutableCacheStatsHolder(levels);
        assertEquals(stats, secondStats);
        ImmutableCacheStatsHolder nonMatchingStats = nonMatchingStatsHolder.getImmutableCacheStatsHolder(levels);
        assertNotEquals(stats, nonMatchingStats);
        ImmutableCacheStatsHolder differentStoreNameStats = differentStoreNameStatsHolder.getImmutableCacheStatsHolder(levels);
        assertNotEquals(stats, differentStoreNameStats);
    }

    public void testGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        DefaultCacheStatsHolder cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(cacheStatsHolder, 10);
        Map<List<String>, CacheStats> expected = DefaultCacheStatsHolderTests.populateStats(
            cacheStatsHolder,
            usedDimensionValues,
            1000,
            10
        );
        ImmutableCacheStatsHolder stats = cacheStatsHolder.getImmutableCacheStatsHolder(dimensionNames.toArray(new String[0]));

        // test the value in the map is as expected for each distinct combination of values
        for (List<String> dimensionValues : expected.keySet()) {
            CacheStats expectedCounter = expected.get(dimensionValues);

            ImmutableCacheStats actualCacheStatsHolder = DefaultCacheStatsHolderTests.getNode(
                dimensionValues,
                cacheStatsHolder.getStatsRoot()
            ).getImmutableStats();
            ImmutableCacheStats actualImmutableCacheStatsHolder = getNode(dimensionValues, stats.getStatsRoot()).getStats();

            assertEquals(expectedCounter.immutableSnapshot(), actualCacheStatsHolder);
            assertEquals(expectedCounter.immutableSnapshot(), actualImmutableCacheStatsHolder);
        }

        // test gets for total (this also checks sum-of-children logic)
        CacheStats expectedTotal = new CacheStats();
        for (List<String> dims : expected.keySet()) {
            expectedTotal.add(expected.get(dims));
        }
        assertEquals(expectedTotal.immutableSnapshot(), stats.getTotalStats());

        assertEquals(expectedTotal.getHits(), stats.getTotalHits());
        assertEquals(expectedTotal.getMisses(), stats.getTotalMisses());
        assertEquals(expectedTotal.getEvictions(), stats.getTotalEvictions());
        assertEquals(expectedTotal.getSizeInBytes(), stats.getTotalSizeInBytes());
        assertEquals(expectedTotal.getItems(), stats.getTotalItems());

        assertSumOfChildrenStats(stats.getStatsRoot());
    }

    public void testEmptyDimsList() throws Exception {
        // If the dimension list is empty, the tree should have only the root node containing the total stats.
        DefaultCacheStatsHolder cacheStatsHolder = new DefaultCacheStatsHolder(List.of(), storeName);
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(cacheStatsHolder, 100);
        DefaultCacheStatsHolderTests.populateStats(cacheStatsHolder, usedDimensionValues, 10, 100);
        ImmutableCacheStatsHolder stats = cacheStatsHolder.getImmutableCacheStatsHolder(null);

        ImmutableCacheStatsHolder.Node statsRoot = stats.getStatsRoot();
        assertEquals(0, statsRoot.children.size());
        assertEquals(stats.getTotalStats(), statsRoot.getStats());
    }

    public void testAggregateByAllDimensions() throws Exception {
        // Aggregating with all dimensions as levels should just give us the same values that were in the original map
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        DefaultCacheStatsHolder statsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStats> expected = DefaultCacheStatsHolderTests.populateStats(statsHolder, usedDimensionValues, 1000, 10);
        ImmutableCacheStatsHolder stats = statsHolder.getImmutableCacheStatsHolder(dimensionNames.toArray(new String[0]));

        for (Map.Entry<List<String>, CacheStats> expectedEntry : expected.entrySet()) {
            List<String> dimensionValues = new ArrayList<>();
            for (String dimValue : expectedEntry.getKey()) {
                dimensionValues.add(dimValue);
            }
            assertEquals(expectedEntry.getValue().immutableSnapshot(), getNode(dimensionValues, stats.statsRoot).getStats());
        }
        assertSumOfChildrenStats(stats.statsRoot);
    }

    public void testAggregateBySomeDimensions() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        DefaultCacheStatsHolder statsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStats> expected = DefaultCacheStatsHolderTests.populateStats(statsHolder, usedDimensionValues, 1000, 10);

        for (int i = 0; i < (1 << dimensionNames.size()); i++) {
            // Test each combination of possible levels
            List<String> levels = new ArrayList<>();
            for (int nameIndex = 0; nameIndex < dimensionNames.size(); nameIndex++) {
                if ((i & (1 << nameIndex)) != 0) {
                    levels.add(dimensionNames.get(nameIndex));
                }
            }

            if (levels.size() == 0) {
                // If we pass empty levels to CacheStatsHolder to aggregate by, we should only get a root node with the total stats in it
                ImmutableCacheStatsHolder stats = statsHolder.getImmutableCacheStatsHolder(levels.toArray(new String[0]));
                assertEquals(statsHolder.getStatsRoot().getImmutableStats(), stats.getStatsRoot().getStats());
                assertEquals(0, stats.getStatsRoot().children.size());
            } else {
                ImmutableCacheStatsHolder stats = statsHolder.getImmutableCacheStatsHolder(levels.toArray(new String[0]));
                Map<List<String>, ImmutableCacheStatsHolder.Node> aggregatedLeafNodes = getAllLeafNodes(stats.statsRoot);

                for (Map.Entry<List<String>, ImmutableCacheStatsHolder.Node> aggEntry : aggregatedLeafNodes.entrySet()) {
                    CacheStats expectedCounter = new CacheStats();
                    for (List<String> expectedDims : expected.keySet()) {
                        if (expectedDims.containsAll(aggEntry.getKey())) {
                            expectedCounter.add(expected.get(expectedDims));
                        }
                    }
                    assertEquals(expectedCounter.immutableSnapshot(), aggEntry.getValue().getStats());
                }
                assertSumOfChildrenStats(stats.statsRoot);
            }
        }
    }

    public void testXContentForLevels() throws Exception {
        List<String> dimensionNames = List.of("A", "B", "C");

        DefaultCacheStatsHolder statsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        DefaultCacheStatsHolderTests.populateStatsHolderFromStatsValueMap(
            statsHolder,
            Map.of(
                List.of("A1", "B1", "C1"),
                new CacheStats(1, 1, 1, 1, 1),
                List.of("A1", "B1", "C2"),
                new CacheStats(2, 2, 2, 2, 2),
                List.of("A1", "B2", "C1"),
                new CacheStats(3, 3, 3, 3, 3),
                List.of("A2", "B1", "C3"),
                new CacheStats(4, 4, 4, 4, 4)
            )
        );
        ImmutableCacheStatsHolder stats = statsHolder.getImmutableCacheStatsHolder(dimensionNames.toArray(new String[0]));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;

        builder.startObject();
        stats.toXContent(builder, params);
        builder.endObject();
        String resultString = builder.toString();
        Map<String, Object> result = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), resultString, true);

        Map<String, BiConsumer<CacheStats, Integer>> fieldNamesMap = Map.of(
            ImmutableCacheStats.Fields.SIZE_IN_BYTES,
            (counter, value) -> counter.sizeInBytes.inc(value),
            ImmutableCacheStats.Fields.EVICTIONS,
            (counter, value) -> counter.evictions.inc(value),
            ImmutableCacheStats.Fields.HIT_COUNT,
            (counter, value) -> counter.hits.inc(value),
            ImmutableCacheStats.Fields.MISS_COUNT,
            (counter, value) -> counter.misses.inc(value),
            ImmutableCacheStats.Fields.ITEM_COUNT,
            (counter, value) -> counter.items.inc(value)
        );

        Map<List<String>, ImmutableCacheStatsHolder.Node> leafNodes = getAllLeafNodes(stats.getStatsRoot());
        for (Map.Entry<List<String>, ImmutableCacheStatsHolder.Node> entry : leafNodes.entrySet()) {
            List<String> xContentKeys = new ArrayList<>();
            for (int i = 0; i < dimensionNames.size(); i++) {
                xContentKeys.add(dimensionNames.get(i));
                xContentKeys.add(entry.getKey().get(i));
            }
            CacheStats counterFromXContent = new CacheStats();

            for (Map.Entry<String, BiConsumer<CacheStats, Integer>> fieldNamesEntry : fieldNamesMap.entrySet()) {
                List<String> fullXContentKeys = new ArrayList<>(xContentKeys);
                fullXContentKeys.add(fieldNamesEntry.getKey());
                int valueInXContent = (int) getValueFromNestedXContentMap(result, fullXContentKeys);
                BiConsumer<CacheStats, Integer> incrementer = fieldNamesEntry.getValue();
                incrementer.accept(counterFromXContent, valueInXContent);
            }

            ImmutableCacheStats expected = entry.getValue().getStats();
            assertEquals(counterFromXContent.immutableSnapshot(), expected);
        }
    }

    public void testXContent() throws Exception {
        // Tests logic of filtering levels out, logic for aggregating by those levels is already covered
        List<String> dimensionNames = List.of("A", "B", "C");
        DefaultCacheStatsHolder statsHolder = new DefaultCacheStatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = DefaultCacheStatsHolderTests.getUsedDimensionValues(statsHolder, 10);
        DefaultCacheStatsHolderTests.populateStats(statsHolder, usedDimensionValues, 100, 10);

        // If the levels in the params are empty or contains only unrecognized levels, we should only see the total stats and no level
        // aggregation
        List<List<String>> levelsList = List.of(List.of(), List.of("D"));
        for (List<String> levels : levelsList) {
            ImmutableCacheStatsHolder stats = statsHolder.getImmutableCacheStatsHolder(levels.toArray(new String[0]));
            ToXContent.Params params = getLevelParams(levels);
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            stats.toXContent(builder, params);
            builder.endObject();

            String resultString = builder.toString();
            Map<String, Object> result = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), resultString, true);

            assertTotalStatsPresentInXContentResponse(result);
            // assert there are no other entries in the map besides these 6
            assertEquals(6, result.size());
        }

        // if we pass recognized levels in any order, alongside ignored unrecognized levels, we should see the above plus level aggregation
        List<String> levels = List.of("C", "A", "E");
        ImmutableCacheStatsHolder stats = statsHolder.getImmutableCacheStatsHolder(levels.toArray(new String[0]));
        ToXContent.Params params = getLevelParams(levels);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, params);
        builder.endObject();

        String resultString = builder.toString();
        Map<String, Object> result = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), resultString, true);
        assertTotalStatsPresentInXContentResponse(result);
        assertNotNull(result.get("A"));
        assertEquals(7, result.size());
    }

    private void assertTotalStatsPresentInXContentResponse(Map<String, Object> result) {
        // assert the total stats are present
        assertNotEquals(0, (int) result.get(ImmutableCacheStats.Fields.SIZE_IN_BYTES));
        assertNotEquals(0, (int) result.get(ImmutableCacheStats.Fields.EVICTIONS));
        assertNotEquals(0, (int) result.get(ImmutableCacheStats.Fields.HIT_COUNT));
        assertNotEquals(0, (int) result.get(ImmutableCacheStats.Fields.MISS_COUNT));
        assertNotEquals(0, (int) result.get(ImmutableCacheStats.Fields.ITEM_COUNT));
        // assert the store name is present
        assertEquals(storeName, (String) result.get(ImmutableCacheStatsHolder.STORE_NAME_FIELD));
    }

    private ToXContent.Params getLevelParams(List<String> levels) {
        Map<String, String> paramMap = new HashMap<>();
        if (!levels.isEmpty()) {
            paramMap.put("level", String.join(",", levels));
        }
        return new ToXContent.MapParams(paramMap);
    }

    public static Object getValueFromNestedXContentMap(Map<String, Object> xContentMap, List<String> keys) {
        Map<String, Object> current = xContentMap;
        for (int i = 0; i < keys.size() - 1; i++) {
            Object next = current.get(keys.get(i));
            if (next == null) {
                return null;
            }
            current = (Map<String, Object>) next;
        }
        return current.get(keys.get(keys.size() - 1));
    }

    // Get a map from the list of dimension values to the corresponding leaf node.
    private Map<List<String>, ImmutableCacheStatsHolder.Node> getAllLeafNodes(ImmutableCacheStatsHolder.Node root) {
        Map<List<String>, ImmutableCacheStatsHolder.Node> result = new HashMap<>();
        getAllLeafNodesHelper(result, root, new ArrayList<>());
        return result;
    }

    private void getAllLeafNodesHelper(
        Map<List<String>, ImmutableCacheStatsHolder.Node> result,
        ImmutableCacheStatsHolder.Node current,
        List<String> pathToCurrent
    ) {
        if (current.children.isEmpty()) {
            result.put(pathToCurrent, current);
        } else {
            for (Map.Entry<String, ImmutableCacheStatsHolder.Node> entry : current.children.entrySet()) {
                List<String> newPath = new ArrayList<>(pathToCurrent);
                newPath.add(entry.getKey());
                getAllLeafNodesHelper(result, entry.getValue(), newPath);
            }
        }
    }

    private ImmutableCacheStatsHolder.Node getNode(List<String> dimensionValues, ImmutableCacheStatsHolder.Node root) {
        ImmutableCacheStatsHolder.Node current = root;
        for (String dimensionValue : dimensionValues) {
            current = current.getChildren().get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private void assertSumOfChildrenStats(ImmutableCacheStatsHolder.Node current) {
        if (!current.children.isEmpty()) {
            CacheStats expectedTotal = new CacheStats();
            for (ImmutableCacheStatsHolder.Node child : current.children.values()) {
                expectedTotal.add(child.getStats());
            }
            assertEquals(expectedTotal.immutableSnapshot(), current.getStats());
            for (ImmutableCacheStatsHolder.Node child : current.children.values()) {
                assertSumOfChildrenStats(child);
            }
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;
import org.opensearch.search.startree.filter.provider.StarTreeFilterProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BoolStarTreeFilterProviderTests extends OpenSearchTestCase {
    private SearchContext searchContext;
    private MapperService mapperService;
    private CompositeDataCubeFieldType compositeFieldType;
    private final String DATE_FORMAT = "strict_date_optional_time||epoch_millis";
    private final String METHOD = "method";
    private final String STATUS = "status";
    private final String PORT = "port";
    private final String ZONE = "zone";
    private final String RESPONSE_TIME = "response_time";
    private final String LATENCY = "latency";
    private final String REGION = "region";
    private final String EVENT_DATE = "event_date";

    @Before
    public void setup() throws IOException {
        // Setup common test dependencies
        searchContext = mock(SearchContext.class);
        mapperService = mock(MapperService.class);

        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        QueryShardContext queryShardContext = new QueryShardContext(
            0,
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            mapperService,
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            null,
            () -> randomNonNegativeLong(),
            null,
            null,
            () -> true,
            null
        );

        List<DateTimeUnitRounding> intervals = Arrays.asList(
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.HOUR_OF_DAY),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.MINUTES_OF_HOUR),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.SECOND_OF_MINUTE)
        );
        // Create composite field type with dimensions
        compositeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(
                    new OrdinalDimension(METHOD),
                    new OrdinalDimension(STATUS),
                    new OrdinalDimension(PORT),
                    new OrdinalDimension(ZONE),
                    new OrdinalDimension(RESPONSE_TIME),
                    new OrdinalDimension(LATENCY),
                    new OrdinalDimension(REGION),
                    new DateDimension(EVENT_DATE, intervals, DateFieldMapper.Resolution.MILLISECONDS)
                ),
                List.of(new Metric("size", List.of(MetricStat.SUM))),
                new StarTreeFieldConfiguration(
                    randomIntBetween(1, 10_000),
                    Collections.emptySet(),
                    StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
                )
            )
        );

        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);
        when(mapperService.getCompositeFieldTypes()).thenReturn(Set.of(compositeFieldType));

        // Setup field types
        KeywordFieldMapper.KeywordFieldType methodType = new KeywordFieldMapper.KeywordFieldType(METHOD);
        NumberFieldMapper.NumberFieldType statusType = new NumberFieldMapper.NumberFieldType(STATUS, NumberFieldMapper.NumberType.INTEGER);
        NumberFieldMapper.NumberFieldType portType = new NumberFieldMapper.NumberFieldType(PORT, NumberFieldMapper.NumberType.INTEGER);
        KeywordFieldMapper.KeywordFieldType zoneType = new KeywordFieldMapper.KeywordFieldType(ZONE);
        NumberFieldMapper.NumberFieldType responseTimeType = new NumberFieldMapper.NumberFieldType(
            RESPONSE_TIME,
            NumberFieldMapper.NumberType.INTEGER
        );
        NumberFieldMapper.NumberFieldType latencyType = new NumberFieldMapper.NumberFieldType(LATENCY, NumberFieldMapper.NumberType.FLOAT);
        KeywordFieldMapper.KeywordFieldType regionType = new KeywordFieldMapper.KeywordFieldType(REGION);
        DateFieldMapper.DateFieldType eventDateFieldType = new DateFieldMapper.DateFieldType(EVENT_DATE);
        when(mapperService.fieldType(METHOD)).thenReturn(methodType);
        when(mapperService.fieldType(STATUS)).thenReturn(statusType);
        when(mapperService.fieldType(PORT)).thenReturn(portType);
        when(mapperService.fieldType(ZONE)).thenReturn(zoneType);
        when(mapperService.fieldType(RESPONSE_TIME)).thenReturn(responseTimeType);
        when(mapperService.fieldType(LATENCY)).thenReturn(latencyType);
        when(mapperService.fieldType(REGION)).thenReturn(regionType);
        when(mapperService.fieldType(EVENT_DATE)).thenReturn(eventDateFieldType); // Map the date field
    }

    public void testMustWithDateFieldRange() throws IOException {
        String fromDateStr = "2023-01-10T10:00:00.000Z";
        String toDateStr = "2023-01-10T12:00:00.000Z";
        long expectedFromMillis = Instant.parse(fromDateStr).toEpochMilli();
        long expectedToMillis = Instant.parse(toDateStr).toEpochMilli() - 1;

        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStr).lt(toDateStr).format(DATE_FORMAT)); // Common OpenSearch date format

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have two dimensions", 2, filter.getDimensions().size());
        assertTrue("Should contain method dimension", filter.getDimensions().contains(METHOD));
        assertTrue("Should contain date-hour dimension", filter.getDimensions().contains(EVENT_DATE));

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify date range filter
        List<DimensionFilter> dateFilters = filter.getFiltersForDimension("event_date");
        assertEquals("Should have one filter for date", 1, dateFilters.size());
        testDateFilters(dateFilters, "event_date_hour", new long[] { expectedFromMillis }, new long[] { expectedToMillis });
    }

    public void testMustWithMultipleDateFieldRanges() throws IOException {
        String fromDateStrHour = "2023-01-10T10:00:00.000Z";
        String toDateStrHour = "2023-01-10T12:00:00.000Z";
        long expectedFromMillisHour = Instant.parse(fromDateStrHour).toEpochMilli();
        long expectedToMillisHour = Instant.parse(toDateStrHour).toEpochMilli() - 1;

        String fromDateStrMin = "2023-01-10T10:30:00.000Z";
        String toDateStrMin = "2023-01-10T12:30:00.000Z";
        long expectedFromMillisMin = Instant.parse(fromDateStrMin).toEpochMilli();
        long expectedToMillisMin = Instant.parse(toDateStrMin).toEpochMilli() - 1;

        String fromDateStrSec = "2023-01-10T08:30:01.000Z";
        String toDateStrSec = "2023-01-10T10:21:31.000Z";
        long expectedFromMillisSec = Instant.parse(fromDateStrSec).toEpochMilli();
        long expectedToMillisSec = Instant.parse(toDateStrSec).toEpochMilli() - 1;

        // first clause will use hour granularity, second clause will use minute granularity, overlapping intervals
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(
            new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrHour).lt(toDateStrHour).format(DATE_FORMAT)
        ).must(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrMin).lt(toDateStrMin).format(DATE_FORMAT));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have 1 dimension", 1, filter.getDimensions().size());
        assertTrue("Should contain date-minute dimension", filter.getDimensions().contains(EVENT_DATE));

        // Verify date range filter
        List<DimensionFilter> dateFilters = filter.getFiltersForDimension(EVENT_DATE);
        assertEquals("Should have 1 filter for date", 1, dateFilters.size());
        testDateFilters(dateFilters, "event_date_minute", new long[] { expectedFromMillisMin }, new long[] { expectedToMillisHour });

        // multiple fields, second date clause will lowe minute granularity, overlapping intervals
        boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrSec).lt(toDateStrSec).format(DATE_FORMAT))
            .must(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrHour).lt(toDateStrHour).format(DATE_FORMAT));

        provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have 2 dimension", 2, filter.getDimensions().size());
        assertTrue("Should contain method dimension", filter.getDimensions().contains(METHOD));
        assertTrue("Should contain date-second dimension", filter.getDimensions().contains(EVENT_DATE));

        // Verify date range filter
        dateFilters = filter.getFiltersForDimension(EVENT_DATE);
        assertEquals("Should have 1 filter for date", 1, dateFilters.size());
        testDateFilters(dateFilters, "event_date_second", new long[] { expectedFromMillisHour }, new long[] { expectedToMillisSec });

        // dis-joint intervals
        boolQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrSec).lt(toDateStrSec).format(DATE_FORMAT))
            .must(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrMin).lt(toDateStrMin).format(DATE_FORMAT));

        provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null", filter);
    }

    public void testShouldWithMultipleDateFieldRanges() throws IOException {
        String fromDateStrHour = "2023-02-10T10:00:00.000Z";
        String toDateStrHour = "2023-02-10T12:00:00.000Z";
        long expectedFromMillisHour = Instant.parse(fromDateStrHour).toEpochMilli();
        long expectedToMillisHour = Instant.parse(toDateStrHour).toEpochMilli() - 1;

        String fromDateStrMin = "2023-01-10T10:30:00.000Z";
        String toDateStrMin = "2023-01-10T12:30:00.000Z";
        long expectedFromMillisMin = Instant.parse(fromDateStrMin).toEpochMilli();
        long expectedToMillisMin = Instant.parse(toDateStrMin).toEpochMilli() - 1;

        String fromDateStrSec = "2023-01-10T14:30:01.000Z";
        String toDateStrSec = "2023-01-10T14:30:31.000Z";
        long expectedFromMillisSec = Instant.parse(fromDateStrSec).toEpochMilli();
        long expectedToMillisSec = Instant.parse(toDateStrSec).toEpochMilli() - 1;

        // first clause will use hour granularity, second clause will use minute granularity
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(
            new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrHour).lt(toDateStrHour).format(DATE_FORMAT)
        ).should(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrMin).lt(toDateStrMin).format(DATE_FORMAT));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have 1 dimension", 1, filter.getDimensions().size());
        assertTrue("Should contain date-minute dimension", filter.getDimensions().contains(EVENT_DATE));

        // Verify date range filter
        long[] expectedLows = new long[] { expectedFromMillisHour, expectedFromMillisMin };
        long[] expectedHighs = new long[] { expectedToMillisHour, expectedToMillisMin };

        List<DimensionFilter> dateFilters = filter.getFiltersForDimension(EVENT_DATE);
        assertEquals("Should have 2 filter for date", 2, dateFilters.size());
        testDateFilters(dateFilters, "event_date_minute", expectedLows, expectedHighs);

        // first date clause will use min granularity, second date clause will use hour granularity
        boolQuery = new BoolQueryBuilder().should(
            new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrMin).lt(toDateStrMin).format(DATE_FORMAT)
        )
            .should(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrHour).lt(toDateStrHour).format(DATE_FORMAT))
            .should(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrSec).lt(toDateStrSec).format(DATE_FORMAT));

        provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have 1 dimension", 1, filter.getDimensions().size());
        assertTrue("Should contain date-second dimension", filter.getDimensions().contains(EVENT_DATE));

        // Verify date range filter
        dateFilters = filter.getFiltersForDimension(EVENT_DATE);
        expectedLows = new long[] { expectedFromMillisMin, expectedFromMillisHour, expectedFromMillisSec };
        expectedHighs = new long[] { expectedToMillisMin, expectedToMillisHour, expectedToMillisSec };

        assertEquals("Should have 3 filters for date", 3, dateFilters.size());
        testDateFilters(dateFilters, "event_date_second", expectedLows, expectedHighs);

        // should on different fields
        boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder(METHOD, "GET"))
            .should(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrMin).lt(toDateStrMin).format(DATE_FORMAT))
            .should(new RangeQueryBuilder(EVENT_DATE).gte(fromDateStrSec).lt(toDateStrSec).format(DATE_FORMAT));

        provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null", filter);
    }

    private void testDateFilters(List<DimensionFilter> dateFilters, String subDimension, long[] expectedLows, long[] expectedHighs) {
        for (int i = 0; i < dateFilters.size(); i++) {
            assertTrue("Date filter should be RangeMatchDimFilter", dateFilters.get(i) instanceof RangeMatchDimFilter);
            RangeMatchDimFilter rf = (RangeMatchDimFilter) dateFilters.get(i);
            assertEquals("Sub-dimension should match", subDimension, rf.getSubDimensionName());
            assertEquals("Date lower bound should match", expectedLows[i], rf.getLow());
            assertEquals("Date upper bound should match", expectedHighs[i], rf.getHigh());
            assertTrue("Date lower bound should be inclusive", rf.isIncludeLow());
            assertTrue("Date upper bound should be inclusive", rf.isIncludeHigh());
        }
    }

    public void testSimpleMustWithMultipleDimensions() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(new TermQueryBuilder(STATUS, 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have two dimensions", 2, filter.getDimensions().size());
        assertTrue("Should contain method dimension", filter.getDimensions().contains(METHOD));
        assertTrue("Should contain status dimension", filter.getDimensions().contains(STATUS));

        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertEquals("Should have one filter for method", 1, methodFilters.size());
        assertTrue("Should be ExactMatchDimFilter", methodFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have one filter for status", 1, statusFilters.size());
        assertTrue("Should be ExactMatchDimFilter", statusFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) statusFilters.getFirst(), 200L);
    }

    public void testNestedMustClauses() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(new BoolQueryBuilder().must(new TermQueryBuilder(STATUS, 200)).must(new TermQueryBuilder(PORT, 443)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filter
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertExactMatchValue((ExactMatchDimFilter) statusFilters.getFirst(), 200L);

        // Verify port filter
        List<DimensionFilter> portFilters = filter.getFiltersForDimension(PORT);
        assertExactMatchValue((ExactMatchDimFilter) portFilters.getFirst(), 443L);
    }

    public void testMustWithDifferentQueryTypes() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(new TermsQueryBuilder(STATUS, Arrays.asList(200, 201)))
            .must(new RangeQueryBuilder(PORT).gte(80).lte(443));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertTrue("Method should be ExactMatchDimFilter", methodFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filter
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertTrue("Status should be ExactMatchDimFilter", statusFilters.getFirst() instanceof ExactMatchDimFilter);
        Set<Object> expectedStatusValues = Set.of(200L, 201L);
        Set<Object> actualStatusValues = new HashSet<>(((ExactMatchDimFilter) statusFilters.getFirst()).getRawValues());
        assertEquals("Status should have expected values", expectedStatusValues, actualStatusValues);

        // Verify port filter
        List<DimensionFilter> portFilters = filter.getFiltersForDimension(PORT);
        assertTrue("Port should be RangeMatchDimFilter", portFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter portRange = (RangeMatchDimFilter) portFilters.getFirst();
        assertEquals("Port lower bound should be 80", 80L, portRange.getLow());
        assertEquals("Port upper bound should be 443", 443L, portRange.getHigh());
        assertTrue("Port bounds should be inclusive", portRange.isIncludeLow() && portRange.isIncludeHigh());
    }

    public void testMustWithSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(STATUS, 200)).must(new TermQueryBuilder(STATUS, 404));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        // this should return null as same dimension in MUST is logically impossible
        assertNull("Filter should be null for same dimension in MUST", filter);
    }

    public void testEmptyMustClause() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for empty bool query", filter);
    }

    public void testShouldWithSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200))
            .should(new TermQueryBuilder(STATUS, 404));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());
        assertTrue("Should contain status dimension", filter.getDimensions().contains(STATUS));

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have two filters for status", 2, statusFilters.size());
        assertTrue("Both should be ExactMatchDimFilter", statusFilters.stream().allMatch(f -> f instanceof ExactMatchDimFilter));

        Set<Object> expectedValues = Set.of(200L, 404L);
        Set<Object> actualValues = new HashSet<>();
        for (DimensionFilter dimensionFilter : statusFilters) {
            actualValues.addAll(((ExactMatchDimFilter) dimensionFilter).getRawValues());
        }
        assertEquals("Should contain expected status values", expectedValues, actualValues);
    }

    public void testShouldWithSameDimensionRange() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new RangeQueryBuilder(STATUS).gte(200).lte(300))
            .should(new RangeQueryBuilder(STATUS).gte(400).lte(500));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have two filters for status", 2, statusFilters.size());
        assertTrue("Both should be RangeMatchDimFilter", statusFilters.stream().allMatch(f -> f instanceof RangeMatchDimFilter));

        // Verify first range
        RangeMatchDimFilter firstRange = (RangeMatchDimFilter) statusFilters.getFirst();
        assertEquals("First range lower bound should be 200", 200L, firstRange.getLow());
        assertEquals("First range upper bound should be 300", 300L, firstRange.getHigh());
        assertTrue("First range lower bound should be inclusive", firstRange.isIncludeLow());
        assertTrue("First range upper bound should be inclusive", firstRange.isIncludeHigh());

        // Verify second range
        RangeMatchDimFilter secondRange = (RangeMatchDimFilter) statusFilters.get(1);
        assertEquals("Second range lower bound should be 400", 400L, secondRange.getLow());
        assertEquals("Second range upper bound should be 500", 500L, secondRange.getHigh());
        assertTrue("Second range lower bound should be inclusive", secondRange.isIncludeLow());
        assertTrue("Second range upper bound should be inclusive", secondRange.isIncludeHigh());
    }

    public void testShouldWithSameDimensionMixed() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200))
            .should(new RangeQueryBuilder(STATUS).gte(400).lte(500));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have two filters for status", 2, statusFilters.size());

        // Find and verify exact match filter
        Optional<ExactMatchDimFilter> exactFilter = statusFilters.stream()
            .filter(f -> f instanceof ExactMatchDimFilter)
            .map(f -> (ExactMatchDimFilter) f)
            .findFirst();
        assertTrue("Should have exact match filter", exactFilter.isPresent());
        assertEquals("Exact match should be 200", 200L, exactFilter.get().getRawValues().get(0));

        // Find and verify range filter
        Optional<RangeMatchDimFilter> rangeFilter = statusFilters.stream()
            .filter(f -> f instanceof RangeMatchDimFilter)
            .map(f -> (RangeMatchDimFilter) f)
            .findFirst();
        assertTrue("Should have range filter", rangeFilter.isPresent());
        assertEquals("Range lower bound should be 400", 400L, rangeFilter.get().getLow());
        assertEquals("Range upper bound should be 500", 500L, rangeFilter.get().getHigh());
        assertTrue("Range lower bound should be inclusive", rangeFilter.get().isIncludeLow());
        assertTrue("Range upper bound should be inclusive", rangeFilter.get().isIncludeHigh());
    }

    public void testShouldWithDifferentDimensions() throws IOException {
        // SHOULD with different dimensions (should be rejected)
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200))
            .should(new TermQueryBuilder(METHOD, "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for SHOULD across different dimensions", filter);
    }

    public void testNestedShouldSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200))
            .should(new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 404)).should(new TermQueryBuilder(STATUS, 500)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have three filters for status", 3, statusFilters.size());
        assertTrue("All should be ExactMatchDimFilter", statusFilters.stream().allMatch(f -> f instanceof ExactMatchDimFilter));

        Set<Object> expectedValues = Set.of(200L, 404L, 500L);
        Set<Object> actualValues = new HashSet<>();
        for (DimensionFilter dimensionFilter : statusFilters) {
            actualValues.addAll(((ExactMatchDimFilter) dimensionFilter).getRawValues());
        }
        assertEquals("Should contain all expected status values", expectedValues, actualValues);
    }

    public void testEmptyShouldClause() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new BoolQueryBuilder());

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for empty SHOULD clause", filter);
    }

    public void testMustContainingShouldSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(STATUS).gte(200).lt(500))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 404)).should(new TermQueryBuilder(STATUS, 403)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have two filters after intersection", 2, statusFilters.size());

        Set<Object> expectedValues = Set.of(403L, 404L);
        Set<Object> actualValues = new HashSet<>();
        for (DimensionFilter dimFilter : statusFilters) {
            assertTrue("Should be ExactMatchDimFilter", dimFilter instanceof ExactMatchDimFilter);
            ExactMatchDimFilter exactFilter = (ExactMatchDimFilter) dimFilter;
            actualValues.addAll(exactFilter.getRawValues());
        }
        assertEquals("Should contain expected status values", expectedValues, actualValues);
    }

    public void testMustContainingShouldDifferentDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200)).should(new TermQueryBuilder(STATUS, 404)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have two dimensions", 2, filter.getDimensions().size());

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertEquals("Method should have one filter", 1, methodFilters.size());
        assertTrue("Should be ExactMatchDimFilter", methodFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filters
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Status should have two filters", 2, statusFilters.size());
        Set<Object> expectedStatusValues = Set.of(200L, 404L);
        Set<Object> actualStatusValues = new HashSet<>();
        for (DimensionFilter dimFilter : statusFilters) {
            assertTrue("Should be ExactMatchDimFilter", dimFilter instanceof ExactMatchDimFilter);
            ExactMatchDimFilter exactFilter = (ExactMatchDimFilter) dimFilter;
            actualStatusValues.addAll(exactFilter.getRawValues());
        }
        assertEquals("Should contain expected status values", expectedStatusValues, actualStatusValues);
    }

    public void testMultipleLevelsMustNesting() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(
                new BoolQueryBuilder().must(new RangeQueryBuilder(STATUS).gte(200).lte(300))
                    .must(new BoolQueryBuilder().must(new TermQueryBuilder(PORT, 443)))
            );

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertEquals("Method should have one filter", 1, methodFilters.size());
        assertTrue("Should be ExactMatchDimFilter", methodFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filter
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Status should have one filter", 1, statusFilters.size());
        assertTrue("Should be RangeMatchDimFilter", statusFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter rangeFilter = (RangeMatchDimFilter) statusFilters.getFirst();
        assertEquals("Lower bound should be 200", 200L, rangeFilter.getLow());
        assertEquals("Upper bound should be 300", 300L, rangeFilter.getHigh());
        assertTrue("Lower bound should be inclusive", rangeFilter.isIncludeLow());
        assertTrue("Upper bound should be inclusive", rangeFilter.isIncludeHigh());

        // Verify port filter
        List<DimensionFilter> portFilters = filter.getFiltersForDimension(PORT);
        assertEquals("Port should have one filter", 1, portFilters.size());
        assertTrue("Should be ExactMatchDimFilter", portFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) portFilters.getFirst(), 443L);
    }

    public void testShouldInsideShouldSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200))
            .should(new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 404)).should(new TermQueryBuilder(STATUS, 500)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have three filters", 3, statusFilters.size());

        Set<Object> expectedValues = Set.of(200L, 404L, 500L);
        Set<Object> actualValues = new HashSet<>();
        for (DimensionFilter dimFilter : statusFilters) {
            assertTrue("Should be ExactMatchDimFilter", dimFilter instanceof ExactMatchDimFilter);
            ExactMatchDimFilter exactFilter = (ExactMatchDimFilter) dimFilter;
            actualValues.addAll(exactFilter.getRawValues());
        }
        assertEquals("Should contain all expected values", expectedValues, actualValues);
    }

    public void testMustInsideShouldDifferentDimensionRejected() throws IOException {
        // MUST inside SHOULD for different dimension (should be rejected)
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(
            new BoolQueryBuilder().must(new TermQueryBuilder(STATUS, 200)).must(new TermQueryBuilder(METHOD, "GET"))
        );

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for MUST inside SHOULD", filter);
    }

    public void testComplexNestedStructure() throws IOException {
        // Complex nested structure with both MUST and SHOULD
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(
                new BoolQueryBuilder().must(new RangeQueryBuilder(PORT).gte(80).lte(443))
                    .must(new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200)).should(new TermQueryBuilder(STATUS, 404)))
            );

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());
        assertTrue("Should contain all dimensions", filter.getDimensions().containsAll(Set.of(METHOD, PORT, STATUS)));
    }

    public void testMaximumNestingDepth() throws IOException {
        // Build a deeply nested bool query
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"));

        BoolQueryBuilder current = boolQuery;
        for (int i = 0; i < 10; i++) { // Test with 10 levels of nesting
            BoolQueryBuilder nested = new BoolQueryBuilder().must(new TermQueryBuilder(STATUS, 200 + i));
            current.must(nested);
            current = nested;
        }

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should not be null", filter);
    }

    public void testAllClauseTypesCombined() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(
                new BoolQueryBuilder().must(new RangeQueryBuilder(PORT).gte(80).lte(443))
                    .must(new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200)).should(new TermQueryBuilder(STATUS, 201)))
            )
            .must(new TermsQueryBuilder(METHOD, Arrays.asList("GET", "POST"))); // This should intersect with first method term

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());

        // Verify method filter (should be intersection of term and terms)
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertEquals("Should have one filter for method after intersection", 1, methodFilters.size());
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify port filter
        List<DimensionFilter> portFilters = filter.getFiltersForDimension(PORT);
        assertTrue("Port should be RangeMatchDimFilter", portFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter portRange = (RangeMatchDimFilter) portFilters.getFirst();
        assertEquals("Port lower bound should be 80", 80L, portRange.getLow());
        assertEquals("Port upper bound should be 443", 443L, portRange.getHigh());

        // Verify status filters
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have two filters for status", 2, statusFilters.size());
        Set<Object> expectedStatusValues = Set.of(200L, 201L);
        Set<Object> actualStatusValues = new HashSet<>();
        for (DimensionFilter statusFilter : statusFilters) {
            actualStatusValues.addAll(((ExactMatchDimFilter) statusFilter).getRawValues());
        }
        assertEquals("Status should have expected values", expectedStatusValues, actualStatusValues);
    }

    public void testEmptyNestedBools() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new BoolQueryBuilder())
            .must(new BoolQueryBuilder().must(new BoolQueryBuilder()));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for empty nested bool queries", filter);
    }

    public void testSingleClauseBoolQueries() throws IOException {
        // Test single MUST clause
        BoolQueryBuilder mustOnly = new BoolQueryBuilder().must(new TermQueryBuilder(STATUS, 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(mustOnly);
        StarTreeFilter filter = provider.getFilter(searchContext, mustOnly, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());
        assertExactMatchValue((ExactMatchDimFilter) filter.getFiltersForDimension(STATUS).get(0), 200L);

        // Test single SHOULD clause
        BoolQueryBuilder shouldOnly = new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200));

        filter = provider.getFilter(searchContext, shouldOnly, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());
        assertExactMatchValue((ExactMatchDimFilter) filter.getFiltersForDimension(STATUS).get(0), 200L);
    }

    public void testDuplicateDimensionsAcrossNesting() throws IOException {
        // Test duplicate dimensions that should be merged/intersected
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(STATUS).gte(200).lte(500))
            .must(new BoolQueryBuilder().must(new RangeQueryBuilder(STATUS).gte(300).lte(400)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have one filter after intersection", 1, statusFilters.size());
        RangeMatchDimFilter rangeFilter = (RangeMatchDimFilter) statusFilters.getFirst();
        assertEquals("Lower bound should be 300", 300L, rangeFilter.getLow());
        assertEquals("Upper bound should be 400", 400L, rangeFilter.getHigh());
        assertTrue("Lower bound should be exclusive", rangeFilter.isIncludeLow());
        assertTrue("Upper bound should be exclusive", rangeFilter.isIncludeHigh());
    }

    public void testKeywordFieldTypeHandling() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermsQueryBuilder(METHOD, Arrays.asList("GET", "POST")))
            .must(new TermQueryBuilder(STATUS, 200))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder(PORT, 80)).should(new TermQueryBuilder(PORT, 9200)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter (keyword term query)
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertExactMatchValue((ExactMatchDimFilter) statusFilters.getFirst(), 200L);

        // Verify method filter (keyword terms query)
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        ExactMatchDimFilter methodFilter = (ExactMatchDimFilter) methodFilters.getFirst();
        Set<Object> expectedMethod = new HashSet<>();
        expectedMethod.add(new BytesRef("GET"));
        expectedMethod.add(new BytesRef("POST"));
        assertEquals(expectedMethod, new HashSet<>(methodFilter.getRawValues()));

        // Verify port filter (keyword SHOULD terms)
        List<DimensionFilter> portFilters = filter.getFiltersForDimension(PORT);
        assertEquals(2, portFilters.size());
        Set<Object> expectedPorts = new HashSet<>();
        expectedPorts.add(80L);
        expectedPorts.add(9200L);
        Set<Object> actualZones = new HashSet<>();
        for (DimensionFilter portFilter : portFilters) {
            actualZones.addAll(((ExactMatchDimFilter) portFilter).getRawValues());
        }
        assertEquals(expectedPorts, actualZones);
    }

    public void testInvalidDimensionNames() throws IOException {
        // Test dimension that doesn't exist in mapping
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("non_existent_field", "value"))
            .must(new TermQueryBuilder(METHOD, "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for non-existent dimension", filter);

        // Test dimension that exists in mapping but not in star tree dimensions
        NumberFieldMapper.NumberFieldType nonStarTreeField = new NumberFieldMapper.NumberFieldType(
            "non_star_tree_field",
            NumberFieldMapper.NumberType.INTEGER
        );
        when(mapperService.fieldType("non_star_tree_field")).thenReturn(nonStarTreeField);

        boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("non_star_tree_field", 100)).must(new TermQueryBuilder(METHOD, "GET"));

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for non-star-tree dimension", filter);
    }

    public void testUnsupportedQueryTypes() throws IOException {
        // Test unsupported query type in MUST
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new WildcardQueryBuilder(METHOD, "GET*"))
            .must(new TermQueryBuilder(STATUS, 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for unsupported query type", filter);

        // Test unsupported query type in SHOULD
        boolQuery = new BoolQueryBuilder().should(new WildcardQueryBuilder(STATUS, "2*")).should(new TermQueryBuilder(STATUS, 404));

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for unsupported query type in SHOULD", filter);
    }

    public void testInvalidFieldTypes() throws IOException {
        // Test with unsupported field type
        WildcardFieldMapper.WildcardFieldType wildcardType = new WildcardFieldMapper.WildcardFieldType("wildcard_field");
        when(mapperService.fieldType("wildcard_field")).thenReturn(wildcardType);

        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("wildcard_field", "value"))
            .must(new TermQueryBuilder(METHOD, "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for unsupported field type", filter);
    }

    public void testInvalidShouldClauses() throws IOException {
        // Test SHOULD clauses with different dimensions
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200))
            .should(new TermQueryBuilder(METHOD, "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for SHOULD with different dimensions", filter);

        // Test nested MUST inside SHOULD
        boolQuery = new BoolQueryBuilder().should(
            new BoolQueryBuilder().must(new TermQueryBuilder(STATUS, 200)).must(new TermQueryBuilder(METHOD, "GET"))
        );

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for MUST inside SHOULD", filter);
    }

    public void testInvalidMustClauses() throws IOException {
        // Test MUST clauses with same dimension
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(STATUS, 200)).must(new TermQueryBuilder(STATUS, 404));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for multiple MUST on same dimension", filter);

        // Test incompatible range intersections
        boolQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(STATUS).gte(200).lt(300))
            .must(new RangeQueryBuilder(STATUS).gte(400).lt(500));

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for non-overlapping ranges", filter);
    }

    public void testMalformedQueries() throws IOException {
        // Test empty bool query
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for empty bool query", filter);

        // Test deeply nested empty bool queries
        boolQuery = new BoolQueryBuilder().must(new BoolQueryBuilder().must(new BoolQueryBuilder().must(new BoolQueryBuilder())));

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for nested empty bool queries", filter);
    }

    public void testComplexMustWithNestedShould() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(new RangeQueryBuilder(PORT).gte(80).lte(443))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200)).should(new RangeQueryBuilder(STATUS).gte(500).lt(600)))
            // Success or 5xx errors
            .must(
                new BoolQueryBuilder().must(
                    new BoolQueryBuilder().should(new TermQueryBuilder(ZONE, "us-east")).should(new TermQueryBuilder(ZONE, "us-west"))
                )
            );

        // Add field type for zone
        KeywordFieldMapper.KeywordFieldType zoneType = new KeywordFieldMapper.KeywordFieldType(ZONE);
        when(mapperService.fieldType(ZONE)).thenReturn(zoneType);

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have four dimensions", 4, filter.getDimensions().size());

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify port range
        List<DimensionFilter> portFilters = filter.getFiltersForDimension(PORT);
        RangeMatchDimFilter portRange = (RangeMatchDimFilter) portFilters.getFirst();
        assertEquals(80L, portRange.getLow());
        assertEquals(443L, portRange.getHigh());
        assertTrue(portRange.isIncludeLow() && portRange.isIncludeHigh());

        // Verify status filters (term OR range)
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals(2, statusFilters.size());
        for (DimensionFilter statusFilter : statusFilters) {
            if (statusFilter instanceof ExactMatchDimFilter) {
                assertEquals(200L, ((ExactMatchDimFilter) statusFilter).getRawValues().getFirst());
            } else {
                RangeMatchDimFilter statusRange = (RangeMatchDimFilter) statusFilter;
                assertEquals(500L, statusRange.getLow());
                assertEquals(599L, statusRange.getHigh());
                assertTrue(statusRange.isIncludeLow());
                assertTrue(statusRange.isIncludeHigh());
            }
        }

        // Verify zone filters
        List<DimensionFilter> zoneFilters = filter.getFiltersForDimension(ZONE);
        assertEquals(2, zoneFilters.size());
        Set<Object> expectedZones = new HashSet<>();
        expectedZones.add(new BytesRef("us-east"));
        expectedZones.add(new BytesRef("us-west"));
        Set<Object> actualZones = new HashSet<>();
        for (DimensionFilter zoneFilter : zoneFilters) {
            actualZones.addAll(((ExactMatchDimFilter) zoneFilter).getRawValues());
        }
        assertEquals(expectedZones, actualZones);
    }

    public void testRangeAndTermCombinations() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(STATUS).gte(200).lt(300))  // 2xx status codes
            .must(new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 201)).should(new TermQueryBuilder(STATUS, 204)))
            // Specific success codes
            .must(new TermQueryBuilder(METHOD, "POST"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "POST");

        // Verify status filters (intersection of range and terms)
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals(2, statusFilters.size());
        Set<Object> expectedStatus = Set.of(201L, 204L);
        Set<Object> actualStatus = new HashSet<>();
        for (DimensionFilter statusFilter : statusFilters) {
            assertTrue(statusFilter instanceof ExactMatchDimFilter);
            actualStatus.addAll(((ExactMatchDimFilter) statusFilter).getRawValues());
        }
        assertEquals(expectedStatus, actualStatus);
    }

    public void testDeepNestedShouldClauses() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(
                new BoolQueryBuilder().should(
                    new BoolQueryBuilder().should(new TermQueryBuilder(RESPONSE_TIME, 100)).should(new TermQueryBuilder(RESPONSE_TIME, 200))
                )
                    .should(
                        new BoolQueryBuilder().should(new TermQueryBuilder(RESPONSE_TIME, 300))
                            .should(new TermQueryBuilder(RESPONSE_TIME, 400))
                    )
            );

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.get(0), "GET");

        // Verify response_time filters (all SHOULD conditions)
        List<DimensionFilter> responseTimeFilters = filter.getFiltersForDimension(RESPONSE_TIME);
        assertEquals(4, responseTimeFilters.size());
        Set<Object> expectedTimes = Set.of(100L, 200L, 300L, 400L);
        Set<Object> actualTimes = new HashSet<>();
        for (DimensionFilter timeFilter : responseTimeFilters) {
            assertTrue(timeFilter instanceof ExactMatchDimFilter);
            actualTimes.addAll(((ExactMatchDimFilter) timeFilter).getRawValues());
        }
        assertEquals(expectedTimes, actualTimes);
    }

    public void testLargeNumberOfClauses() throws IOException {
        // Create a bool query with large number of SHOULD clauses
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"));

        // Add 100 SHOULD clauses for status
        BoolQueryBuilder statusShould = new BoolQueryBuilder();
        for (int i = 200; i < 300; i++) {
            statusShould.should(new TermQueryBuilder(STATUS, i));
        }
        boolQuery.must(statusShould);

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify filters
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals(100, statusFilters.size());
    }

    public void testMustInsideShould() throws IOException {
        // Test valid case - all clauses on same dimension
        BoolQueryBuilder validBoolQuery = new BoolQueryBuilder().should(
            new BoolQueryBuilder().must(new RangeQueryBuilder(STATUS).gte(200).lt(300)).must(new TermQueryBuilder(STATUS, 201))
        ).should(new TermQueryBuilder(STATUS, 404));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(validBoolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, validBoolQuery, compositeFieldType);

        assertNotNull("Filter should not be null for same dimension", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertEquals("Should have two filters", 2, statusFilters.size());
        Set<Object> expectedValues = Set.of(201L, 404L);
        Set<Object> actualValues = new HashSet<>();
        for (DimensionFilter dimFilter : statusFilters) {
            assertTrue("Should be ExactMatchDimFilter", dimFilter instanceof ExactMatchDimFilter);
            actualValues.addAll(((ExactMatchDimFilter) dimFilter).getRawValues());
        }
        assertEquals("Should contain expected values", expectedValues, actualValues);

        // Test invalid case - multiple dimensions in MUST inside SHOULD
        BoolQueryBuilder invalidBoolQuery = new BoolQueryBuilder().should(
            new BoolQueryBuilder().must(new TermQueryBuilder(STATUS, 200)).must(new TermQueryBuilder(METHOD, "GET"))
        ).should(new TermQueryBuilder(STATUS, 404));

        filter = provider.getFilter(searchContext, invalidBoolQuery, compositeFieldType);
        assertNull("Filter should be null for multiple dimensions in MUST inside SHOULD", filter);
    }

    public void testCombinedMustAndFilterClauses() throws IOException {
        // Test combination of MUST and FILTER clauses
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .filter(new TermQueryBuilder(STATUS, 200))
            .filter(new RangeQueryBuilder(PORT).gte(80).lte(443));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());

        // Verify method filter (from MUST)
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filter (from FILTER)
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertExactMatchValue((ExactMatchDimFilter) statusFilters.getFirst(), 200L);

        // Verify port filter (from FILTER)
        List<DimensionFilter> portFilters = filter.getFiltersForDimension(PORT);
        RangeMatchDimFilter portRange = (RangeMatchDimFilter) portFilters.getFirst();
        assertEquals(80L, portRange.getLow());
        assertEquals(443L, portRange.getHigh());
        assertTrue(portRange.isIncludeLow() && portRange.isIncludeHigh());
    }

    public void testNestedBoolWithMustAndFilter() throws IOException {
        // Test nested bool query with both MUST and FILTER clauses
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(new BoolQueryBuilder().filter(new RangeQueryBuilder(STATUS).gte(200).lt(300)).must(new TermQueryBuilder(STATUS, 201)))
            // Should intersect with range
            .filter(new TermQueryBuilder(PORT, 443));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension(METHOD);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filter (intersection of range and term)
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension(STATUS);
        assertExactMatchValue((ExactMatchDimFilter) statusFilters.getFirst(), 201L);

        // Verify port filter
        List<DimensionFilter> portFilters = filter.getFiltersForDimension(PORT);
        assertExactMatchValue((ExactMatchDimFilter) portFilters.getFirst(), 443L);
    }

    public void testInvalidMustAndFilterCombination() throws IOException {
        // Test invalid combination - same dimension in MUST and FILTER
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(STATUS, 200))
            .filter(new TermQueryBuilder(STATUS, 404));  // Different value for same dimension

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for conflicting conditions", filter);
    }

    public void testKeywordRanges() throws IOException {
        BoolQueryBuilder keywordRangeQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(REGION).gte("eu-").lt("eu-z"))
            // Range of region codes
            .must(new TermQueryBuilder(METHOD, "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(keywordRangeQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, keywordRangeQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        List<DimensionFilter> regionFilters = filter.getFiltersForDimension(REGION);
        assertTrue(regionFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter regionRange = (RangeMatchDimFilter) regionFilters.getFirst();
        assertEquals(new BytesRef("eu-"), regionRange.getLow());
        assertEquals(new BytesRef("eu-z"), regionRange.getHigh());
        assertTrue(regionRange.isIncludeLow());
        assertFalse(regionRange.isIncludeHigh());
    }

    public void testFloatRanges() throws IOException {
        BoolQueryBuilder floatRangeQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(LATENCY).gte(0.5f).lte(2.0f))
            .must(new TermQueryBuilder(STATUS, 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(floatRangeQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, floatRangeQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        List<DimensionFilter> latencyFilters = filter.getFiltersForDimension(LATENCY);
        assertTrue(latencyFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter latencyRange = (RangeMatchDimFilter) latencyFilters.getFirst();
        assertEquals(NumericUtils.floatToSortableInt(0.5f), ((Number) latencyRange.getLow()).floatValue(), 0.0001);
        assertEquals(NumericUtils.floatToSortableInt(2.0f), ((Number) latencyRange.getHigh()).floatValue(), 0.0001);
        assertTrue(latencyRange.isIncludeLow());
        assertTrue(latencyRange.isIncludeHigh());

        // Test combined ranges in SHOULD
        BoolQueryBuilder combinedRangeQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(
                new BoolQueryBuilder().should(new RangeQueryBuilder(LATENCY).gte(0.0).lt(1.0))
                    .should(new RangeQueryBuilder(LATENCY).gte(2.0).lt(3.0))
            );

        filter = provider.getFilter(searchContext, combinedRangeQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        latencyFilters = filter.getFiltersForDimension(LATENCY);
        assertEquals(2, latencyFilters.size());
        for (DimensionFilter dimFilter : latencyFilters) {
            assertTrue(dimFilter instanceof RangeMatchDimFilter);
        }
    }

    public void testFloatRanges_Exclusive() throws IOException {
        // Test float range with different inclusivity combinations
        BoolQueryBuilder floatRangeQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(LATENCY).gt(0.5).lt(2.0))
            // exclusive bounds
            .must(new TermQueryBuilder(STATUS, 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(floatRangeQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, floatRangeQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        List<DimensionFilter> latencyFilters = filter.getFiltersForDimension(LATENCY);
        assertTrue(latencyFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter latencyRange = (RangeMatchDimFilter) latencyFilters.getFirst();

        // For exclusive bounds (gt/lt), we need to use next/previous float values
        long expectedLow = NumericUtils.floatToSortableInt(FloatPoint.nextUp(0.5f));
        long expectedHigh = NumericUtils.floatToSortableInt(FloatPoint.nextDown(2.0f));

        assertEquals(expectedLow, ((Number) latencyRange.getLow()).longValue());
        assertEquals(expectedHigh, ((Number) latencyRange.getHigh()).longValue());
        assertTrue(latencyRange.isIncludeLow());   // After using nextUp, bound becomes inclusive
        assertTrue(latencyRange.isIncludeHigh());  // After using nextDown, bound becomes inclusive
    }

    public void testFloatRanges_Intersection() throws IOException {
        // Test float range with different inclusivity combinations
        BoolQueryBuilder floatRangeQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(LATENCY).gt(0.5).lt(2.0))
            .must(new RangeQueryBuilder(LATENCY).gte(0.6).lt(1.8))
            .must(new TermQueryBuilder(STATUS, 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(floatRangeQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, floatRangeQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        List<DimensionFilter> latencyFilters = filter.getFiltersForDimension(LATENCY);
        assertTrue(latencyFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter latencyRange = (RangeMatchDimFilter) latencyFilters.getFirst();

        // For exclusive bounds (gt/lt), we need to use next/previous float values
        long expectedLow = NumericUtils.floatToSortableInt(0.6f);
        long expectedHigh = NumericUtils.floatToSortableInt(FloatPoint.nextDown(1.8f));

        assertEquals(expectedLow, ((Number) latencyRange.getLow()).longValue());
        assertEquals(expectedHigh, ((Number) latencyRange.getHigh()).longValue());
        assertTrue(latencyRange.isIncludeLow());
        assertTrue(latencyRange.isIncludeHigh());
    }

    public void testKeywordRangeEdgeCases() throws IOException {
        // Test unbounded ranges
        BoolQueryBuilder unboundedQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(REGION).gt("eu-"))  // No upper bound
            .must(new TermQueryBuilder(STATUS, 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(unboundedQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, unboundedQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        List<DimensionFilter> regionFilters = filter.getFiltersForDimension(REGION);
        RangeMatchDimFilter regionRange = (RangeMatchDimFilter) regionFilters.get(0);
        assertEquals(new BytesRef("eu-"), regionRange.getLow());
        assertNull(regionRange.getHigh());  // Unbounded high
        assertFalse(regionRange.isIncludeLow());
        assertTrue(regionRange.isIncludeHigh());

        // Test range intersection
        BoolQueryBuilder intersectionQuery = new BoolQueryBuilder().must(new RangeQueryBuilder(REGION).gte("eu-").lt("eu-z"))
            .must(new RangeQueryBuilder(REGION).gt("eu-a").lte("eu-m"));

        filter = provider.getFilter(searchContext, intersectionQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        regionFilters = filter.getFiltersForDimension(REGION);
        regionRange = (RangeMatchDimFilter) regionFilters.get(0);
        assertEquals(new BytesRef("eu-a"), regionRange.getLow());
        assertEquals(new BytesRef("eu-m"), regionRange.getHigh());
        assertFalse(regionRange.isIncludeLow());
        assertTrue(regionRange.isIncludeHigh());
    }

    public void testMinimumShouldMatch() throws IOException {
        // Test with minimum_should_match set
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200))
            .should(new TermQueryBuilder(STATUS, 404))
            .minimumShouldMatch(2);  // Explicitly set minimum_should_match

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null when minimum_should_match is set", filter);

        // Test nested bool with minimum_should_match
        BoolQueryBuilder nestedBoolQuery = new BoolQueryBuilder().must(new TermQueryBuilder(METHOD, "GET"))
            .must(
                new BoolQueryBuilder().should(new TermQueryBuilder(STATUS, 200))
                    .should(new TermQueryBuilder(STATUS, 404))
                    .minimumShouldMatch(1)
            );  // Set in nested bool

        filter = provider.getFilter(searchContext, nestedBoolQuery, compositeFieldType);
        assertNull("Filter should be null when minimum_should_match is set in nested query", filter);
    }

    public void testMustNotClauseReturnsNull() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().mustNot(new TermQueryBuilder(STATUS, 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        // this should return null as must not clause is not supported
        assertNull("Filter should be null for same dimension in MUST", filter);
    }

    // Helper methods for assertions
    private void assertExactMatchValue(ExactMatchDimFilter filter, String expectedValue) {
        assertEquals(new BytesRef(expectedValue), filter.getRawValues().getFirst());
    }

    private void assertExactMatchValue(ExactMatchDimFilter filter, Long expectedValue) {
        assertEquals(expectedValue, filter.getRawValues().getFirst());
    }
}

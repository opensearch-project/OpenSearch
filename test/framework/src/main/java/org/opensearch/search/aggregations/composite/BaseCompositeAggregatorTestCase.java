/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.composite;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.text.Text;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for the Aggregator Tests which are registered under Composite Aggregation.
 */
public class BaseCompositeAggregatorTestCase extends AggregatorTestCase {

    protected static List<MappedFieldType> FIELD_TYPES;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        FIELD_TYPES = new ArrayList<>();
        FIELD_TYPES.add(new KeywordFieldMapper.KeywordFieldType("keyword"));
        FIELD_TYPES.add(new NumberFieldMapper.NumberFieldType("long", NumberFieldMapper.NumberType.LONG));
        FIELD_TYPES.add(new NumberFieldMapper.NumberFieldType("double", NumberFieldMapper.NumberType.DOUBLE));
        FIELD_TYPES.add(new DateFieldMapper.DateFieldType("date", DateFormatter.forPattern("yyyy-MM-dd||epoch_millis")));
        FIELD_TYPES.add(new NumberFieldMapper.NumberFieldType("price", NumberFieldMapper.NumberType.INTEGER));
        FIELD_TYPES.add(new KeywordFieldMapper.KeywordFieldType("terms"));
        FIELD_TYPES.add(new IpFieldMapper.IpFieldType("ip"));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        FIELD_TYPES = null;
    }

    @Override
    protected MapperService mapperServiceMock() {
        MapperService mapperService = mock(MapperService.class);
        DocumentMapper mapper = mock(DocumentMapper.class);
        when(mapper.typeText()).thenReturn(new Text("_doc"));
        when(mapper.type()).thenReturn("_doc");
        when(mapperService.documentMapper()).thenReturn(mapper);
        return mapperService;
    }

    protected static Map<String, List<Object>> createDocument(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, List<Object>> map = new HashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            String field = (String) fields[i];
            if (fields[i + 1] instanceof List) {
                map.put(field, (List<Object>) fields[i + 1]);
            } else {
                map.put(field, Collections.singletonList(fields[i + 1]));
            }
        }
        return map;
    }

    protected void testSearchCase(
        List<Query> queries,
        List<Map<String, List<Object>>> dataset,
        Supplier<CompositeAggregationBuilder> create,
        Consumer<InternalComposite> verify
    ) throws IOException {
        for (Query query : queries) {
            executeTestCase(false, false, query, dataset, create, verify);
            executeTestCase(false, true, query, dataset, create, verify);
        }
    }

    protected void executeTestCase(
        boolean forceMerge,
        boolean useIndexSort,
        Query query,
        List<Map<String, List<Object>>> dataset,
        Supplier<CompositeAggregationBuilder> create,
        Consumer<InternalComposite> verify
    ) throws IOException {
        Map<String, MappedFieldType> types = FIELD_TYPES.stream().collect(Collectors.toMap(MappedFieldType::name, Function.identity()));
        CompositeAggregationBuilder aggregationBuilder = create.get();
        Sort indexSort = useIndexSort ? buildIndexSort(aggregationBuilder.sources(), types) : null;
        IndexSettings indexSettings = createIndexSettings(indexSort);
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = newIndexWriterConfig(random(), new MockAnalyzer(random()));
            if (indexSort != null) {
                config.setIndexSort(indexSort);
                config.setCodec(TestUtil.getDefaultCodec());
            }
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, config)) {
                Document document = new Document();
                int id = 0;
                for (Map<String, List<Object>> fields : dataset) {
                    document.clear();
                    addToDocument(id, document, fields);
                    indexWriter.addDocument(document);
                    id++;
                }
                if (forceMerge || rarely()) {
                    // forceMerge randomly or if the collector-per-leaf testing stuff would break the tests.
                    indexWriter.forceMerge(1);
                } else {
                    if (dataset.size() > 0) {
                        int numDeletes = randomIntBetween(1, 25);
                        for (int i = 0; i < numDeletes; i++) {
                            id = randomIntBetween(0, dataset.size() - 1);
                            indexWriter.deleteDocuments(new Term("id", Integer.toString(id)));
                            document.clear();
                            addToDocument(id, document, dataset.get(id));
                            indexWriter.addDocument(document);
                        }
                    }

                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                InternalComposite composite = searchAndReduce(
                    indexSettings,
                    indexSearcher,
                    query,
                    aggregationBuilder,
                    FIELD_TYPES.toArray(new MappedFieldType[0])
                );
                verify.accept(composite);
            }
        }
    }

    protected void addToDocument(int id, Document doc, Map<String, List<Object>> keys) {
        doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
        for (Map.Entry<String, List<Object>> entry : keys.entrySet()) {
            final String name = entry.getKey();
            for (Object value : entry.getValue()) {
                if (value instanceof Integer) {
                    doc.add(new SortedNumericDocValuesField(name, (int) value));
                    doc.add(new IntPoint(name, (int) value));
                } else if (value instanceof Long) {
                    doc.add(new SortedNumericDocValuesField(name, (long) value));
                    doc.add(new LongPoint(name, (long) value));
                } else if (value instanceof Double) {
                    doc.add(new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong((double) value)));
                    doc.add(new DoublePoint(name, (double) value));
                } else if (value instanceof String) {
                    doc.add(new SortedSetDocValuesField(name, new BytesRef((String) value)));
                    doc.add(new StringField(name, new BytesRef((String) value), Field.Store.NO));
                } else if (value instanceof InetAddress) {
                    doc.add(new SortedSetDocValuesField(name, new BytesRef(InetAddressPoint.encode((InetAddress) value))));
                    doc.add(new InetAddressPoint(name, (InetAddress) value));
                } else {
                    if (!addValueToDocument(doc, name, value)) throw new AssertionError(
                        "invalid object: " + value.getClass().getSimpleName()
                    );
                }
            }
        }
    }

    /**
     * Override this function to handle any specific type of value you want to add in the document for doing the
     * composite aggregation. If you have added another Composite Aggregation Type then you must override this
     * function so that your field value can be added in the document correctly.
     *
     * @param doc {@link Document}
     * @param name {@link String} Field Name
     * @param value {@link Object} Field value
     * @return boolean true or false, based on if value is added or not
     */
    protected boolean addValueToDocument(final Document doc, final String name, final Object value) {
        return false;
    }

    protected static Sort buildIndexSort(List<CompositeValuesSourceBuilder<?>> sources, Map<String, MappedFieldType> fieldTypes) {
        List<SortField> sortFields = new ArrayList<>();
        Map<String, MappedFieldType> remainingFieldTypes = new HashMap<>(fieldTypes);
        for (CompositeValuesSourceBuilder<?> source : sources) {
            MappedFieldType type = fieldTypes.remove(source.field());
            remainingFieldTypes.remove(source.field());
            SortField sortField = sortFieldFrom(type);
            if (sortField == null) {
                break;
            }
            sortFields.add(sortField);
        }
        while (remainingFieldTypes.size() > 0 && randomBoolean()) {
            // Add extra unused sorts
            List<String> fields = new ArrayList<>(remainingFieldTypes.keySet());
            Collections.sort(fields);
            String field = fields.get(between(0, fields.size() - 1));
            SortField sortField = sortFieldFrom(remainingFieldTypes.remove(field));
            if (sortField != null) {
                sortFields.add(sortField);
            }
        }
        return sortFields.size() > 0 ? new Sort(sortFields.toArray(new SortField[0])) : null;
    }

    protected static SortField sortFieldFrom(MappedFieldType type) {
        if (type instanceof KeywordFieldMapper.KeywordFieldType) {
            return new SortedSetSortField(type.name(), false);
        } else if (type instanceof DateFieldMapper.DateFieldType) {
            return new SortedNumericSortField(type.name(), SortField.Type.LONG, false);
        } else if (type instanceof NumberFieldMapper.NumberFieldType) {
            switch (type.typeName()) {
                case "byte":
                case "short":
                case "integer":
                    return new SortedNumericSortField(type.name(), SortField.Type.INT, false);
                case "long":
                    return new SortedNumericSortField(type.name(), SortField.Type.LONG, false);
                case "float":
                case "double":
                    return new SortedNumericSortField(type.name(), SortField.Type.DOUBLE, false);
                default:
                    return null;
            }
        }
        return null;
    }

    protected static IndexSettings createIndexSettings(Sort sort) {
        Settings.Builder builder = Settings.builder();
        if (sort != null) {
            String[] fields = Arrays.stream(sort.getSort()).map(SortField::getField).toArray(String[]::new);
            String[] orders = Arrays.stream(sort.getSort()).map((o) -> o.getReverse() ? "desc" : "asc").toArray(String[]::new);
            builder.putList("index.sort.field", fields);
            builder.putList("index.sort.order", orders);
        }
        return IndexSettingsModule.newIndexSettings(new Index("_index", "0"), builder.build());
    }

    protected static Map<String, Object> createAfterKey(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            String field = (String) fields[i];
            map.put(field, fields[i + 1]);
        }
        return map;
    }

    protected static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }
}

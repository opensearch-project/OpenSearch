package com.parquet.parquetdataformat.converter;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.search.Query;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting between OpenSearch field types and Arrow/Parquet types.
 * 
 * <p>This converter provides bidirectional mapping between OpenSearch's field type system
 * and Apache Arrow's type system, which serves as the bridge to Parquet data representation.
 * It handles the complete conversion pipeline from OpenSearch indexed data to columnar
 * Parquet storage format.
 * 
 * <p>Supported type conversions:
 * <ul>
 *   <li>OpenSearch numeric types (long, integer, short, byte, double, float) → Arrow Int/FloatingPoint</li>
 *   <li>OpenSearch boolean → Arrow Bool</li>
 *   <li>OpenSearch date → Arrow Timestamp</li>
 *   <li>OpenSearch text/keyword → Arrow Utf8</li>
 * </ul>
 * 
 * <p>The converter also provides reverse mapping capabilities to reconstruct OpenSearch
 * field types from Arrow types, enabling proper schema reconstruction during read operations.
 * 
 * <p>All conversion methods are static and thread-safe, making them suitable for concurrent
 * use across multiple writer instances.
 */
public class FieldTypeConverter {
    
    public static Map<FieldType, Object> convertToArrowFieldMap(MappedFieldType mappedFieldType, Object value) {
        Map<FieldType, Object> fieldMap = new HashMap<>();
        FieldType arrowFieldType = convertToArrowFieldType(mappedFieldType);
        fieldMap.put(arrowFieldType, value);
        return fieldMap;
    }
    
    public static FieldType convertToArrowFieldType(MappedFieldType mappedFieldType) {
        ArrowType arrowType = getArrowType(mappedFieldType.typeName());
        return new FieldType(true, arrowType, null);
    }
    
    public static ParquetFieldType convertToParquetFieldType(MappedFieldType mappedFieldType) {
        ArrowType arrowType = getArrowType(mappedFieldType.typeName());
        return new ParquetFieldType(mappedFieldType.name(), arrowType);
    }
    
    public static MappedFieldType convertToMappedFieldType(String name, ArrowType arrowType) {
        String opensearchType = getOpenSearchType(arrowType);
        return new MockMappedFieldType(name, opensearchType);
    }
    
    private static ArrowType getArrowType(String opensearchType) {
        switch (opensearchType) {
            case "long":
                return new ArrowType.Int(64, true);
            case "integer":
                return new ArrowType.Int(32, true);
            case "short":
                return new ArrowType.Int(16, true);
            case "byte":
                return new ArrowType.Int(8, true);
            case "double":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "float":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "boolean":
                return new ArrowType.Bool();
            case "date":
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            default:
                return new ArrowType.Utf8();
        }
    }
    
    private static String getOpenSearchType(ArrowType arrowType) {
        switch (arrowType) {
            case ArrowType.Int intType -> {
                return switch (intType.getBitWidth()) {
                    case 8 -> "byte";
                    case 16 -> "short";
                    case 32 -> "integer";
                    case 64 -> "long";
                    default -> "integer";
                };
            }
            case ArrowType.FloatingPoint fpType -> {
                return fpType.getPrecision() == FloatingPointPrecision.DOUBLE ? "double" : "float";
            }
            case ArrowType.Bool bool -> {
                return "boolean";
            }
            case ArrowType.Timestamp timestamp -> {
                return "date";
            }
            case null, default -> {
                return "text";
            }
        }
    }
    
    private static class MockMappedFieldType extends MappedFieldType {
        private final String type;
        
        public MockMappedFieldType(String name, String type) {
            super(name, true, false, false, TextSearchInfo.NONE, null);
            this.type = type;
        }
        
        @Override
        public String typeName() {
            return type;
        }
        
        @Override
        public ValueFetcher valueFetcher(org.opensearch.index.query.QueryShardContext context,
                                         org.opensearch.search.lookup.SearchLookup searchLookup,
                                         String format) {
            return null;
        }
        
        @Override
        public Query termQuery(Object value, org.opensearch.index.query.QueryShardContext context) {
            return null;
        }
    }
}

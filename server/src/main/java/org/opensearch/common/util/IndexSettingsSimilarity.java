package org.opensearch.client.opensearch.indices;

import jakarta.json.stream.JsonGenerator;
import java.util.function.Function;
import org.opensearch.client.json.JsonEnum;
import org.opensearch.client.json.JsonpDeserializable;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.json.ObjectBuilderDeserializer;
import org.opensearch.client.json.ObjectDeserializer;
import org.opensearch.client.util.ApiTypeHelper;
import org.opensearch.client.util.ObjectBuilder;
import org.opensearch.client.util.ObjectBuilderBase;
import org.opensearch.client.util.TaggedUnion;



@JsonpDeserializable
public class IndexSettingsSimilarity implements TaggedUnion<IndexSettingsSimilarity.Kind, IndexSettingsSimilarityVariant>, JsonpSerializable {


    public enum Kind implements JsonEnum {
        Bm25("BM25"),

        Boolean("boolean"),

        ;

        private final String jsonValue;

        Kind(String jsonValue) {
            this.jsonValue = jsonValue;
        }

        public String jsonValue() {
            return this.jsonValue;
        }

    }

    private final Kind _kind;
    private final IndexSettingsSimilarityVariant _value;

    public final Kind _kind() {
        return _kind;
    }

    public final IndexSettingsSimilarityVariant _get() {
        return _value;
    }

    public IndexSettingsSimilarity(IndexSettingsSimilarityVariant value) {

        this._kind = ApiTypeHelper.requireNonNull(value._settingsSimilarityKind(), this, "<variant kind>");
        this._value = ApiTypeHelper.requireNonNull(value, this, "<variant value>");

    }

    private IndexSettingsSimilarity(Builder builder) {

        this._kind = ApiTypeHelper.requireNonNull(builder._kind, builder, "<variant kind>");
        this._value = ApiTypeHelper.requireNonNull(builder._value, builder, "<variant value>");

    }

    public static IndexSettingsSimilarity of(Function<Builder, ObjectBuilder<IndexSettingsSimilarity>> fn) {
        return fn.apply(new Builder()).build();
    }


    public void serialize(JsonGenerator generator, JsonpMapper mapper) {
        mapper.serialize(_value, generator);
    }

    public static class Builder extends ObjectBuilderBase implements ObjectBuilder<IndexSettingsSimilarity> {
        private Kind _kind;
        private IndexSettingsSimilarityVariant _value;

        public ObjectBuilder<IndexSettingsSimilarity> bm25(IndexSettingsSimilarityBm25 v) {
            this._kind = Kind.Bm25;
            this._value = v;
            return this;
        }

        public ObjectBuilder<IndexSettingsSimilarity> bm25(
                Function<IndexSettingsSimilarityBm25.Builder, ObjectBuilder<IndexSettingsSimilarityBm25>> fn) {
            return this.bm25(fn.apply(new IndexSettingsSimilarityBm25.Builder()).build());
        }

        public ObjectBuilder<IndexSettingsSimilarity> boolean_(IndexSettingsSimilarityBoolean v) {
            this._kind = Kind.Boolean;
            this._value = v;
            return this;
        }

        public ObjectBuilder<IndexSettingsSimilarity> boolean_(
                Function<IndexSettingsSimilarityBoolean.Builder, ObjectBuilder<IndexSettingsSimilarityBoolean>> fn) {
            return this.boolean_(fn.apply(new IndexSettingsSimilarityBoolean.Builder()).build());
        }


        @Override
        public IndexSettingsSimilarity build() {
            _checkSingleUse();
            return new IndexSettingsSimilarity(this);
        }
    }

    public static final JsonpDeserializer<IndexSettingsSimilarity> _DESERIALIZER = ObjectBuilderDeserializer
            .lazy(Builder::new, IndexSettingsSimilarity::setupIndexSettingsSimilarityDeserializer, Builder::build);

    protected static void setupIndexSettingsSimilarityDeserializer(ObjectDeserializer<Builder> op) {

        op.add(Builder::bm25, IndexSettingsSimilarityBm25._DESERIALIZER, "BM25");
        op.add(Builder::boolean_, IndexSettingsSimilarityBoolean._DESERIALIZER, "boolean");

        op.setTypeProperty("type", null);

    }

}

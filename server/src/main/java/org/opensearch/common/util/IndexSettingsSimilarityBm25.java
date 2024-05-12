package org.opensearch.client.opensearch.indices;

import jakarta.json.stream.JsonGenerator;
import javax.annotation.Nullable;
import java.util.function.Function;
import org.opensearch.client.json.JsonpDeserializable;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.json.ObjectBuilderDeserializer;
import org.opensearch.client.json.ObjectDeserializer;
import org.opensearch.client.util.ObjectBuilder;
import org.opensearch.client.util.ObjectBuilderBase;


@JsonpDeserializable
public class IndexSettingsSimilarityBm25 implements IndexSettingsSimilarityVariant, JsonpSerializable {

    @Nullable
    private final Double b;

    @Nullable
    private final Boolean discountOverlaps;

    @Nullable
    private final Double k1;


    private IndexSettingsSimilarityBm25(Builder builder) {

        this.b = builder.b;
        this.discountOverlaps = builder.discountOverlaps;
        this.k1 = builder.k1;

    }

    public static IndexSettingsSimilarityBm25 of(Function<Builder, ObjectBuilder<IndexSettingsSimilarityBm25>> fn) {
        return fn.apply(new Builder()).build();
    }


    @Override
    public IndexSettingsSimilarity.Kind _settingsSimilarityKind() {
        return IndexSettingsSimilarity.Kind.Bm25;
    }

    @Override
    public void serialize(JsonGenerator generator, JsonpMapper mapper) {
        generator.writeStartObject();
        serializeInternal(generator, mapper);
        generator.writeEnd();
    }

    protected void serializeInternal(JsonGenerator generator, JsonpMapper mapper) {

        generator.write("type", "BM25");

        if (this.b != null) {
            generator.writeKey("b");
            generator.write(this.b);

        }
        if (this.discountOverlaps != null) {
            generator.writeKey("discount_overlaps");
            generator.write(this.discountOverlaps);

        }
        if (this.k1 != null) {
            generator.writeKey("k1");
            generator.write(this.k1);

        }

    }

    /**
     * Builder for {@link IndexSettingsSimilarityBm25}.
     */

    public static class Builder extends ObjectBuilderBase implements ObjectBuilder<IndexSettingsSimilarityBm25> {

        @Nullable
        private Double b;

        @Nullable
        private Boolean discountOverlaps;

        @Nullable
        private Double k1;

        /**
         * API name: {@code b}
         */
        public final Builder b(@Nullable Double value) {
            this.b = value;
            return this;
        }

        /**
         * API name: {@code discount_overlaps}
         */
        public final Builder discountOverlaps(@Nullable Boolean value) {
            this.discountOverlaps = value;
            return this;
        }

        /**
         * API name: {@code k1}
         */
        public final Builder k1(@Nullable Double value) {
            this.k1 = value;
            return this;
        }

        /**
         * Builds a {@link IndexSettingsSimilarityBm25}.
         *
         * @throws NullPointerException if some of the required fields are null.
         */

        public IndexSettingsSimilarityBm25 build() {
            _checkSingleUse();

            return new IndexSettingsSimilarityBm25(this);
        }
    }

    public static final JsonpDeserializer<IndexSettingsSimilarityBm25> _DESERIALIZER = ObjectBuilderDeserializer.lazy(
            Builder::new,
            IndexSettingsSimilarityBm25::setupIndexSettingsSimilarityBm25Deserializer
    );

    protected static void setupIndexSettingsSimilarityBm25Deserializer(ObjectDeserializer<IndexSettingsSimilarityBm25.Builder> op) {

        op.add(Builder::b, JsonpDeserializer.doubleDeserializer(), "b");
        op.add(Builder::discountOverlaps, JsonpDeserializer.booleanDeserializer(), "discount_overlaps");
        op.add(Builder::k1, JsonpDeserializer.doubleDeserializer(), "k1");

        op.ignore("type");
    }

}

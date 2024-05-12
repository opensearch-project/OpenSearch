package org.opensearch.client.opensearch.indices;

import jakarta.json.stream.JsonGenerator;
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
public class IndexSettingsSimilarityBoolean implements IndexSettingsSimilarityVariant, JsonpSerializable {

    private IndexSettingsSimilarityBoolean(Builder builder) {

    }

    public static IndexSettingsSimilarityBoolean of(Function<Builder, ObjectBuilder<IndexSettingsSimilarityBoolean>> fn) {
        return fn.apply(new Builder()).build();
    }


    @Override
    public IndexSettingsSimilarity.Kind _settingsSimilarityKind() {
        return IndexSettingsSimilarity.Kind.Boolean;
    }

    @Override
    public void serialize(JsonGenerator generator, JsonpMapper mapper) {
        generator.writeStartObject();
        serializeInternal(generator, mapper);
        generator.writeEnd();
    }

    protected void serializeInternal(JsonGenerator generator, JsonpMapper mapper) {

        generator.write("type", "boolean");

    }


    /**
     * Builder for {@link IndexSettingsSimilarityBoolean}.
     */

    public static class Builder extends ObjectBuilderBase implements ObjectBuilder<IndexSettingsSimilarityBoolean> {

        public IndexSettingsSimilarityBoolean build() {
            _checkSingleUse();

            return new IndexSettingsSimilarityBoolean(this);
        }
    }


    public static final JsonpDeserializer<IndexSettingsSimilarityBoolean> _DESERIALIZER = ObjectBuilderDeserializer.lazy(
            Builder::new,
            IndexSettingsSimilarityBoolean::setupIndexSettingsSimilarityBooleanDeserializer
    );

    protected static void setupIndexSettingsSimilarityBooleanDeserializer(ObjectDeserializer<IndexSettingsSimilarityBoolean.Builder> op) {

        op.ignore("type");
    }
}

package org.opensearch.client.opensearch.indices;

import org.opensearch.client.json.JsonpSerializable;

public interface IndexSettingsSimilarityVariant extends JsonpSerializable {

    IndexSettingsSimilarity.Kind _settingsSimilarityKind();

    default IndexSettingsSimilarity _toSettingsSimilarity() {
        return new IndexSettingsSimilarity(this);
    }

}

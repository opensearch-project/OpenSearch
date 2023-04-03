package org.opensearch.index.mapper;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.fielddata.*;
import org.opensearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetBytesLeafFieldData;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import sun.security.krb5.KrbException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.*;
import static sun.security.krb5.Config.refresh;

public class FlatObjectFieldDataTests extends AbstractFieldDataTestCase {
    private String FIELD_TYPE = "flat_object";

    @Override
    protected boolean hasDocValues() {
        return true;
    }

    public void testDocValue() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("test")
                .startObject("properties")
                .startObject("field")
                .field("type", FIELD_TYPE)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        ); final DocumentMapper mapper = mapperService.documentMapperParser().parse("test", new CompressedXContent(mapping));

        XContentBuilder json =
            XContentFactory.jsonBuilder().startObject().startObject("field").field("foo", "bar").endObject().endObject()
        ;
        ParsedDocument d = mapper.parse(new SourceToParse("test", "1",  BytesReference.bytes(json) , XContentType.JSON));
        writer.addDocument(d.rootDoc());
        writer.commit();

        IndexFieldData<?> fieldData = getForField("field");
        List<LeafReaderContext> readers = refreshReader();
        assertEquals(1, readers.size());
    }

    @Override
    protected String getFieldDataType() {
        return FIELD_TYPE;
    }
}

/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.rest.action.cat;

import org.opensearch.client.node.ProtobufNodeClient;
import org.opensearch.common.Table;
import org.opensearch.common.io.Streams;
import org.opensearch.common.io.UTF8StreamWriter;
import org.opensearch.core.common.io.stream.BytesStream;
import org.opensearch.rest.ProtobufBaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.rest.action.cat.RestTable.buildHelpWidths;
import static org.opensearch.rest.action.cat.RestTable.pad;

/**
 * Base Transport action class for _cat API
*
* @opensearch.api
*/
public abstract class ProtobufAbstractCatAction extends ProtobufBaseRestHandler {

    protected abstract RestChannelConsumer doCatRequest(RestRequest request, ProtobufNodeClient client);

    protected abstract void documentation(StringBuilder sb);

    protected abstract Table getTableWithHeader(RestRequest request);

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final ProtobufNodeClient client) throws IOException {
        boolean helpWanted = request.paramAsBoolean("help", false);
        if (helpWanted) {
            return channel -> {
                Table table = getTableWithHeader(request);
                int[] width = buildHelpWidths(table, request);
                BytesStream bytesOutput = Streams.flushOnCloseStream(channel.bytesOutput());
                UTF8StreamWriter out = new UTF8StreamWriter().setOutput(bytesOutput);
                for (Table.Cell cell : table.getHeaders()) {
                    // need to do left-align always, so create new cells
                    pad(new Table.Cell(cell.value), width[0], request, out);
                    out.append(" | ");
                    pad(new Table.Cell(cell.attr.containsKey("alias") ? cell.attr.get("alias") : ""), width[1], request, out);
                    out.append(" | ");
                    pad(new Table.Cell(cell.attr.containsKey("desc") ? cell.attr.get("desc") : "not available"), width[2], request, out);
                    out.append("\n");
                }
                out.close();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, bytesOutput.bytes()));
            };
        } else {
            return doCatRequest(request, client);
        }
    }

    static Set<String> RESPONSE_PARAMS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("format", "h", "v", "ts", "pri", "bytes", "size", "time", "s", "timeout"))
    );

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

}

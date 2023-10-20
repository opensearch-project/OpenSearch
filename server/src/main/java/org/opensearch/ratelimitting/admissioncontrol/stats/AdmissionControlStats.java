/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.stats;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class AdmissionControlStats implements ToXContentFragment, Writeable {

    List<BaseAdmissionControllerStats> admissionControllerStatsList;

    /**
     *
     * @param admissionControllerStatsList list of admissionControllerStats
     */
    public AdmissionControlStats(List<BaseAdmissionControllerStats> admissionControllerStatsList){
        this.admissionControllerStatsList = admissionControllerStatsList;
    }

    /**
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public AdmissionControlStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.admissionControllerStatsList = in.readNamedWriteableList(BaseAdmissionControllerStats.class);
        } else {
            this.admissionControllerStatsList = null;
        }
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out the output stream to write entity content to
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeList(this.admissionControllerStatsList);
        }
    }

    /**
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("admission_control");
        this.admissionControllerStatsList.forEach(stats -> {
            try {
                builder.field(stats.getWriteableName(), stats);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return builder.endObject();
    }
}

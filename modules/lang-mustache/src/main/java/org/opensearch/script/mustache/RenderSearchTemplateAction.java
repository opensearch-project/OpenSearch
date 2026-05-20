/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script.mustache;

import org.opensearch.action.ActionType;

public class RenderSearchTemplateAction extends ActionType<SearchTemplateResponse> {

    public static final RenderSearchTemplateAction INSTANCE = new RenderSearchTemplateAction();
    public static final String NAME = "indices:data/read/search/template/render";

    private RenderSearchTemplateAction() {
        super(NAME, SearchTemplateResponse::new);
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.percolator;

import org.apache.lucene.search.Query;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A filter for a field based on several terms matching on any of them.
 */
public class SavedQueryQueryBuilder extends AbstractQueryBuilder<SavedQueryQueryBuilder> {

    public static final String NAME = "saved_query";

    public static final ParseField INDEX_NAME = new ParseField("index_name");
    public static final ParseField DOC_TYPE = new ParseField("doc_type");
    public static final ParseField ID = new ParseField("id");
    public static final ParseField FIELD_NAME = new ParseField("field_name");

    private static ConstructingObjectParser<SavedQueryQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new SavedQueryQueryBuilder((String) a[0], (String) a[1], (String) a[2], (String) a[3]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DOC_TYPE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_NAME);
    }

    public static SavedQueryQueryBuilder fromXContent(QueryParseContext parseContext) {
        return PARSER.apply(parseContext.parser(), null);
    }

    private String indexName;
    private String docType;
    private String id;
    private String fieldName;

    public SavedQueryQueryBuilder(String indexName, String docType, String id, String fieldName) {
        this.indexName = indexName;
        this.docType = docType;
        this.id = id;
        this.fieldName = fieldName;
    }

    public SavedQueryQueryBuilder(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readString(), in.readString());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeString(docType);
        out.writeString(id);
        out.writeString(fieldName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(INDEX_NAME.getPreferredName(), indexName);
        builder.field(DOC_TYPE.getPreferredName(), docType);
        builder.field(ID.getPreferredName(), id);
        builder.field(FIELD_NAME.getPreferredName(), fieldName);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        throw new UnsupportedOperationException("This query only rewrites to another query, it should never be executed directly.");
    }

    @Override
    protected boolean doEquals(SavedQueryQueryBuilder other) {
        return Objects.equals(indexName, other.indexName) && 
                Objects.equals(docType, other.docType) && 
                Objects.equals(id, other.id) && 
                Objects.equals(fieldName, other.fieldName);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(indexName, docType, id);
    }

    private QueryBuilder fetch(String indexName, String docType, String id, String fieldName, Client client, NamedXContentRegistry namedXContentRegistry) throws IOException {
        GetRequest getRequest = new GetRequest(indexName, docType, id).preference("_local")
                .fetchSourceContext(new FetchSourceContext(true, new String[] { fieldName }, new String[0]));
        final GetResponse getResponse = client.get(getRequest).actionGet();
        if (getResponse.isExists() == false || getResponse.isSourceEmpty() == true) {
            throw new ResourceNotFoundException("Cannot find saved query document with id [{}], doc_type [{}], index [{}]", indexName,
                    docType, id);
        }
        try (XContentParser parser = XContentHelper.createParser(namedXContentRegistry, getResponse.getSourceAsBytesRef())) {
            XContentParser.Token currentToken = parser.nextToken();
            if (currentToken != XContentParser.Token.START_OBJECT) {
                throw new IllegalStateException(
                        "Malformed saved query in document with name [" + getRequest.id() + "] in " + fieldName + " field");
            }
            while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (currentToken == XContentParser.Token.FIELD_NAME) {
                    if (parser.currentName().equals(fieldName)) {
                        QueryParseContext parseContext = new QueryParseContext(parser);
                        return parseContext.parseInnerQueryBuilder();
                    }
                } else {
                    parser.skipChildren();
                }
            }
            throw new IllegalStateException("Document with name [" + getRequest.id() + "] found but missing " + fieldName + " field");
        }
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        // The rewrite always loads the saved query and return it so if we got here we always need to rewrite
        return fetch(indexName, docType, id, fieldName, queryRewriteContext.getClient(), queryRewriteContext.getXContentRegistry());
    }

}

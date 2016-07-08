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

package org.elasticsearch.search.aggregations.metrics.kmeans;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.GeoPointValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource.GeoPoint;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class GeoKMeansParser extends GeoPointValuesSourceParser {

    public GeoKMeansParser() {
        super(false, false);
    }

    @Override
    protected ValuesSourceAggregationBuilder<GeoPoint, ?> createFactory(String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        GeoKMeansAggregationBuilder builder = new GeoKMeansAggregationBuilder(aggregationName);
        Integer numClusters = (Integer) otherOptions.get(GeoKMeansAggregationBuilder.CLUSTERS_FIELD);
        if (numClusters != null) {
            builder.setNumClusters(numClusters);
        }
        Double maxStreamClusterCoeff = (Double) otherOptions.get(GeoKMeansAggregationBuilder.MAX_STREAM_CLUSTERS_COEFF_FIELD);
        if (maxStreamClusterCoeff != null) {
            builder.setMaxStreamingClustersCoeff(maxStreamClusterCoeff);
        }
        Double distCutoffMultiplier = (Double) otherOptions.get(GeoKMeansAggregationBuilder.DIST_CUTOFF_MULTIPLIER_FIELD);
        if (distCutoffMultiplier != null) {
            builder.setMaxStreamingClustersCoeff(distCutoffMultiplier);
        }
        return builder;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (token.isValue()) {
            if (parseFieldMatcher.match(currentFieldName, GeoKMeansAggregationBuilder.CLUSTERS_FIELD)) {
                otherOptions.put(GeoKMeansAggregationBuilder.CLUSTERS_FIELD, parser.intValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, GeoKMeansAggregationBuilder.MAX_STREAM_CLUSTERS_COEFF_FIELD)) {
                otherOptions.put(GeoKMeansAggregationBuilder.MAX_STREAM_CLUSTERS_COEFF_FIELD, parser.doubleValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, GeoKMeansAggregationBuilder.DIST_CUTOFF_MULTIPLIER_FIELD)) {
                otherOptions.put(GeoKMeansAggregationBuilder.DIST_CUTOFF_MULTIPLIER_FIELD, parser.doubleValue());
                return true;
            }
        }
        return false;
    }

}

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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.GeoPoint;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Objects;

public class GeoKMeansAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource.GeoPoint, GeoKMeansAggregationBuilder> {

    public static final ParseField CLUSTERS_FIELD = new ParseField("clusters");
    public static final ParseField MAX_STREAM_CLUSTERS_COEFF_FIELD = new ParseField("max_clusters_coeff");
    public static final ParseField DIST_CUTOFF_MULTIPLIER_FIELD = new ParseField("distance_cutoff_multiplier");
    public static final ParseField AGGREGATION_NAME_FIED = new ParseField(InternalGeoKMeans.NAME);

    private int numClusters = 20;
    private double maxStreamingClustersCoeff = 1;
    private double distanceCutoffCoeffMultiplier = 2;

    public GeoKMeansAggregationBuilder(String name) {
        super(name, InternalGeoKMeans.TYPE, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
    }

    public GeoKMeansAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalGeoKMeans.TYPE, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
        numClusters = in.readVInt();
        maxStreamingClustersCoeff = in.readDouble();
        distanceCutoffCoeffMultiplier = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(numClusters);
        out.writeDouble(maxStreamingClustersCoeff);
        out.writeDouble(distanceCutoffCoeffMultiplier);
    }

    @Override
    public String getWriteableName() {
        return AGGREGATION_NAME_FIED.getPreferredName();
    }

    /**
     * Set the num of clusters required.
     */
    public GeoKMeansAggregationBuilder setNumClusters(int numClusters) {
        if (numClusters <= 0) {
            throw new IllegalArgumentException("[clusters] must be greater than 1 in [" + name + "]");
        }
        this.numClusters = numClusters;
        return this;
    }

    /**
     * Get the num of clusters required.
     */
    public int getNumClusters() {
        return numClusters;
    }

    public GeoKMeansAggregationBuilder setMaxStreamingClustersCoeff(double maxStreamingClustersCoeff) {
        this.maxStreamingClustersCoeff = maxStreamingClustersCoeff;
        return this;
    }

    public double getMaxStreamingClustersCoeff() {
        return maxStreamingClustersCoeff;
    }

    public GeoKMeansAggregationBuilder setDistanceCutoffCoeffMultiplier(double distanceCutoffCoeffMultiplier) {
        this.distanceCutoffCoeffMultiplier = distanceCutoffCoeffMultiplier;
        return this;
    }

    public double getDistanceCutoffCoeffMultiplier() {
        return distanceCutoffCoeffMultiplier;
    }

    @Override
    protected ValuesSourceAggregatorFactory<GeoPoint, ?> innerBuild(AggregationContext context, ValuesSourceConfig<GeoPoint> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new GeoKMeansAggregatorFactory(name, numClusters, maxStreamingClustersCoeff, distanceCutoffCoeffMultiplier, type, config,
                context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CLUSTERS_FIELD.getPreferredName(), numClusters);
        builder.field(MAX_STREAM_CLUSTERS_COEFF_FIELD.getPreferredName(), maxStreamingClustersCoeff);
        builder.field(DIST_CUTOFF_MULTIPLIER_FIELD.getPreferredName(), distanceCutoffCoeffMultiplier);
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(numClusters, maxStreamingClustersCoeff, distanceCutoffCoeffMultiplier);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        GeoKMeansAggregationBuilder other = (GeoKMeansAggregationBuilder) obj;
        return Objects.equals(numClusters, other.numClusters) &&
                Objects.equals(maxStreamingClustersCoeff, other.maxStreamingClustersCoeff) &&
                Objects.equals(distanceCutoffCoeffMultiplier, other.distanceCutoffCoeffMultiplier);
    }

}

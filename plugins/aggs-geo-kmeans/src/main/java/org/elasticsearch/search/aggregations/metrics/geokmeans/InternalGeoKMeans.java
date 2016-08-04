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

package org.elasticsearch.search.aggregations.metrics.geokmeans;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InternalGeoKMeans extends InternalMetricsAggregation implements GeoKMeans {

    public static final String NAME = "geo_kmeans";
    public static final Type TYPE = new Type(NAME);

    private final List<Cluster> clusters;
    private final long totalNumPoints;
    private final int k;

    public InternalGeoKMeans(String name, int k, List<Cluster> clusters, long totalNumPoints, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.k = k;
        this.clusters = clusters;
        this.totalNumPoints = totalNumPoints;
    }

    public InternalGeoKMeans(StreamInput in) throws IOException {
        super(in);
        this.k = in.readVInt();
        int size = in.readVInt();
        List<Cluster> clusters = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            clusters.add(new InternalCluster(in));
        }
        this.clusters = clusters;
        this.totalNumPoints = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(k);
        out.writeVInt(clusters.size());
        for (Cluster cluster : clusters) {
            cluster.writeTo(out);
        }
        out.writeVLong(totalNumPoints);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public List<Cluster> getClusters() {
        return clusters;
    }

    public long getTotalNumPoints() {
        return totalNumPoints;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Cluster> combinedResults = new ArrayList<>();
        long totalNumPoints = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoKMeans internalGeoKMeans = (InternalGeoKMeans) aggregation;
            combinedResults.addAll(internalGeoKMeans.getClusters());
            totalNumPoints += internalGeoKMeans.getTotalNumPoints();
        }
        if (combinedResults.size() > 0) {
            Collections.sort(combinedResults, new Comparator<Cluster>() {

                @Override
                public int compare(Cluster o1, Cluster o2) {
                    return (int) (o2.getDocCount() - o1.getDocCount());
                }
            });
            StandardKMeans kmeans = new StandardKMeans(k, combinedResults);
            for (int i = 0; i < 100; i++) {
                kmeans.nextIteration();
            }
            List<Cluster> results = kmeans.getResults();
            Collections.sort(results, new Comparator<Cluster>() {

                @Override
                public int compare(Cluster o1, Cluster o2) {
                    return (int) (o2.getDocCount() - o1.getDocCount());
                }
            });
            return new InternalGeoKMeans(getName(), k, results, totalNumPoints, pipelineAggregators(), metaData);
        } else {
            return new InternalGeoKMeans(getName(), k, combinedResults, totalNumPoints, pipelineAggregators(), metaData);
        }
    }

    @Override
    public Object getProperty(List<String> path) {
        return null;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("clusters");
        for (Cluster cluster : clusters) {
            cluster.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("points_count", totalNumPoints);
        return builder;
    }

    public static class InternalCluster implements Writeable, ToXContent, Cluster {

        private GeoPoint centroid;
        private long docCount;
        private GeoPoint topLeft;
        private GeoPoint bottomRight;
        private Set<GeoPoint> points = null;

        public InternalCluster(GeoPoint centroid, long docCount, GeoPoint topLeft, GeoPoint bottomRight) {
            this.centroid = centroid;
            this.docCount = docCount;
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        public InternalCluster(StreamInput in) throws IOException {
            this.centroid = new GeoPoint(in.readDouble(), in.readDouble());
            this.docCount = in.readVLong();
            this.topLeft = new GeoPoint(in.readDouble(), in.readDouble());
            this.bottomRight = new GeoPoint(in.readDouble(), in.readDouble());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(centroid.getLat());
            out.writeDouble(centroid.getLon());
            out.writeVLong(docCount);
            out.writeDouble(topLeft.getLat());
            out.writeDouble(topLeft.getLon());
            out.writeDouble(bottomRight.getLat());
            out.writeDouble(bottomRight.getLon());
        }

        @Override
        public Set<GeoPoint> points() {
            return points;
        }

        public void points(Set<GeoPoint> points) {
            this.points = points;
        }

        @Override
        public GeoPoint getCentroid() {
            return centroid;
        }

        @Override
        public GeoPoint getTopLeft() {
            return topLeft;
        }

        @Override
        public GeoPoint getBottomRight() {
            return bottomRight;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("centroid");
            builder.field("lat", centroid.getLat());
            builder.field("lon", centroid.getLon());
            builder.endObject();
            builder.field(CommonFields.DOC_COUNT, docCount);
            builder.startObject("top_left");
            builder.field("lat", topLeft.getLat());
            builder.field("lon", topLeft.getLon());
            builder.endObject();
            builder.startObject("bottom_right");
            builder.field("lat", bottomRight.getLat());
            builder.field("lon", bottomRight.getLon());
            builder.endObject();
            if (points != null) {
                builder.startArray("points");
                for (GeoPoint point : points) {
                    builder.startObject();
                    builder.field("lat", point.getLat());
                    builder.field("lon", point.getLon());
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

    }

}

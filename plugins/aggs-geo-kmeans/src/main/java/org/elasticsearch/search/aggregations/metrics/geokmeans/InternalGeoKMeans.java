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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalGeoKMeans extends InternalMultiBucketAggregation<InternalGeoKMeans, InternalGeoKMeans.Bucket>
        implements GeoKMeans {

    public static final String NAME = "geo_kmeans";

    private final List<Bucket> buckets;
    private final long totalNumPoints;
    private final int k;

    public InternalGeoKMeans(String name, int k, List<Bucket> buckets, long totalNumPoints, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.k = k;
        this.buckets = buckets;
        this.totalNumPoints = totalNumPoints;
    }

    public InternalGeoKMeans(StreamInput in) throws IOException {
        super(in);
        this.k = in.readVInt();
        int size = in.readVInt();
        List<Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in));
        }
        this.buckets = buckets;
        this.totalNumPoints = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(k);
        out.writeVInt(buckets.size());
        for (Bucket bucket : buckets) {
            bucket.writeTo(out);
        }
        out.writeVLong(totalNumPoints);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public List<Bucket> getBuckets() {
        return buckets;
    }

    public long getTotalNumPoints() {
        return totalNumPoints;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Bucket> combinedResults = new ArrayList<>();
        long totalNumPoints = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoKMeans internalGeoKMeans = (InternalGeoKMeans) aggregation;
            combinedResults.addAll(internalGeoKMeans.getBuckets());
            totalNumPoints += internalGeoKMeans.getTotalNumPoints();
        }
        if (combinedResults.size() > 0) {
            Collections.sort(combinedResults, new Comparator<Bucket>() {

                @Override
                public int compare(Bucket o1, Bucket o2) {
                    return (int) (o2.getDocCount() - o1.getDocCount());
                }
            });
            StandardKMeans kmeans = new StandardKMeans(k, combinedResults);
            for (int i = 0; i < 100; i++) {
                kmeans.nextIteration();
            }
            List<Bucket> results = kmeans.getResults(reduceContext);
            Collections.sort(results, new Comparator<Bucket>() {

                @Override
                public int compare(Bucket o1, Bucket o2) {
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
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("points_count", totalNumPoints);
        return builder;
    }

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements GeoKMeans.Bucket {

        private final GeoPoint centroid;
        private final long docCount;
        private final InternalAggregations aggregations;

        public Bucket(GeoPoint centroid, long docCount, InternalAggregations aggregations) {
            if (centroid == null) {
                throw new AssertionError("Tried to store a null centroid in a bucket");
            }
            this.centroid = centroid;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in) throws IOException {
            centroid = in.readGeoPoint();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGeoPoint(centroid);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public Object getKey() {
            return centroid;
        }

        @Override
        public String getKeyAsString() {
            return "[" + centroid.lon() + ", " + centroid.lat() + "]";
        }

        @Override
        public GeoPoint getCentroid() {
            return centroid;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString());
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(centroid, docCount, aggregations);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != this.getClass()) {
                return false;
            }
            Bucket other = (Bucket) obj;
            return Objects.equals(centroid, other.centroid) &&
                    Objects.equals(docCount, other.docCount) &&
                    Objects.equals(aggregations, other.aggregations);
        }

    }

    @Override
    protected int doHashCode() {
        return Objects.hash(buckets, k, totalNumPoints);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalGeoKMeans other = (InternalGeoKMeans) obj;
        return Objects.equals(k, other.k) &&
                Objects.equals(totalNumPoints, other.totalNumPoints) &&
                Objects.equals(buckets, other.buckets);
    }

    @Override
    public InternalGeoKMeans create(List<Bucket> buckets) {
        return new InternalGeoKMeans(NAME, k, buckets, totalNumPoints, pipelineAggregators(), metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.centroid, prototype.docCount, aggregations);
    }

}

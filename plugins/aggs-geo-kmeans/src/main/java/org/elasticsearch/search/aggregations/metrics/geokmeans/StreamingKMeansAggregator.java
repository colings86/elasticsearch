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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.geokmeans.GeoKMeans.Cluster;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class StreamingKMeansAggregator extends MetricsAggregator {

    private final StreamingKMeans kmeans;
    private ValuesSource.GeoPoint valuesSource;

    public StreamingKMeansAggregator(String name, AggregatorFactories factories, int numClusters, double maxStreamingClustersCoeff,
            double distanceCutoffCoeffMultiplier, ValuesSource.GeoPoint valuesSource, AggregationContext aggregationContext,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        // NOCOMMIT check that is indeed valid to use the shardID as the random
        // seed
        Random random = new Random(aggregationContext.searchContext().indexShard().shardId().hashCode());
        BigArrays bigArrays = aggregationContext.bigArrays();
        this.kmeans = new StreamingKMeans(numClusters, maxStreamingClustersCoeff, distanceCutoffCoeffMultiplier, random, bigArrays);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final MultiGeoPointValues values = valuesSource.geoPointValues(ctx);
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                values.setDocument(doc);
                final int valuesCount = values.count();

                for (int i = 0; i < valuesCount; ++i) {
                    final GeoPoint val = new GeoPoint(values.valueAt(i));
                    kmeans.collectPoint(val, sub, doc);
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        List<Cluster> clusters = kmeans.getClusters();
        if (clusters == null) {
            return buildEmptyAggregation();
        }
        return new InternalGeoKMeans(name, kmeans.getNumFinalClusters(), clusters, kmeans.getNumPoints(), pipelineAggregators(),
                metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoKMeans(name, kmeans.getNumFinalClusters(), Collections.emptyList(), 0, pipelineAggregators(), metaData());
    }

}

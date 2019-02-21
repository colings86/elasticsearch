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
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.MergingBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class StreamingKMeansAggregator extends DeferableBucketAggregator {

    private final int numFinalClusters;
    private final double maxStreamingClustersCoeff;
    private final double distanceCutoffCoeffMultiplier;
    private ValuesSource.GeoPoint valuesSource;
    private double distanceCutoffCoeff;
    private ObjectArray<GeoPoint> centroids;
    private int numPoints;
    private Random random;
    private long numClusters;
    private MergingBucketsDeferringCollector deferringCollector;

    public StreamingKMeansAggregator(String name, AggregatorFactories factories, int numFinalClusters, double maxStreamingClustersCoeff,
            double distanceCutoffCoeffMultiplier, ValuesSource.GeoPoint valuesSource, SearchContext context, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.numFinalClusters = numFinalClusters;
        this.maxStreamingClustersCoeff = maxStreamingClustersCoeff;
        this.distanceCutoffCoeffMultiplier = distanceCutoffCoeffMultiplier;
        this.valuesSource = valuesSource;
        // NOCOMMIT check that is indeed valid to use the shardID as the random seed
        this.random = new Random(context.indexShard().shardId().hashCode());

        this.numPoints = 0;
        this.numClusters = 0;
        this.distanceCutoffCoeff = 1;
        this.centroids = context.bigArrays().newObjectArray(1);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        deferringCollector = new MergingBucketsDeferringCollector(context);
        return deferringCollector;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final MultiGeoPointValues values = valuesSource.geoPointValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    for (int i = 0; i < valueCount; i++) {
                        final GeoPoint val = new GeoPoint(values.nextValue());
                        collectPoint(val, sub, doc);
                    }
                }
            }
        };
    }

    public void collectPoint(GeoPoint point, LeafBucketCollector sub, int doc) throws IOException {
        double maxStreamingClusters = maxStreamingClustersCoeff * numFinalClusters * Math.log(numPoints);
        if (numPoints > 1 && numClusters > maxStreamingClusters) {
            mergeClusters();
        }
        numPoints++;
        MinDistanceResult minDistResult = calculateMinDistance(point, centroids, numClusters);
        int nearestCentroidIndex = minDistResult.nearestCentroidIndex;
        double minDistance = minDistResult.distance;
        double f = distanceCutoffCoeff / (numFinalClusters * (1 + Math.log(numPoints)));
        double probability = minDistance / f;
        if (numPoints == 1 || random.nextDouble() <= probability) {
            // create new centroid
            centroids = context.bigArrays().grow(centroids, numClusters + 1);
            centroids.set(numClusters, point);
            collectBucket(sub, doc, numClusters);
            numClusters++;
        } else {
            // add to existing centroid
            collectExistingBucket(sub, doc, nearestCentroidIndex);
            long newDocCount = getDocCounts().get(nearestCentroidIndex);
            GeoPoint existingCentroid = centroids.get(nearestCentroidIndex);
            double newMeanLat = existingCentroid.getLat() + (point.getLat() - existingCentroid.getLat()) / newDocCount;
            double newMeanLon = existingCentroid.getLon() + (point.getLon() - existingCentroid.getLon()) / newDocCount;
            GeoPoint newCentroid = new GeoPoint(newMeanLat, newMeanLon);
            centroids.set(nearestCentroidIndex, newCentroid);
        }
    }

    private void mergeClusters() {
        double maxStreamingClusters = maxStreamingClustersCoeff * numFinalClusters * Math.log(numPoints);
        while (numClusters > maxStreamingClusters) {
            distanceCutoffCoeff *= distanceCutoffCoeffMultiplier;
            try (ObjectArray<GeoPoint> oldCentroids = centroids) {
                ObjectArray<GeoPoint> newCentroids = context.bigArrays().newObjectArray(1);
                long[] mergeMap = new long[(int) oldCentroids.size()];
                int newNumClusters = 1;
                newCentroids.set(0, oldCentroids.get(0));
                mergeMap[0] = 0;
                for (int i = 1; i < numClusters; i++) {
                    GeoPoint centroid = oldCentroids.get(i);
                    long docCount = bucketDocCount(i);
                    MinDistanceResult minDistResult = calculateMinDistance(centroid, newCentroids, newNumClusters);
                    int nearestCentroidIndex = minDistResult.nearestCentroidIndex;
                    double minDistance = minDistResult.distance;
                    double f = distanceCutoffCoeff / (numFinalClusters * (1 + Math.log(numPoints)));
                    double probability = docCount * minDistance / f;
                    if (random.nextDouble() <= probability) {
                        newCentroids = context.bigArrays().grow(newCentroids, newNumClusters + 1);
                        newCentroids.set(newNumClusters, centroid);
                        mergeMap[i] = newNumClusters;
                        newNumClusters++;
                    } else {
                        long newDocCount = bucketDocCount(nearestCentroidIndex) + docCount;
                        GeoPoint existingCentroid = newCentroids.get(nearestCentroidIndex);
                        double newMeanLat = existingCentroid.getLat()
                                + docCount * (centroid.getLat() - existingCentroid.getLat()) / newDocCount;
                        double newMeanLon = existingCentroid.getLon()
                                + docCount * (centroid.getLon() - existingCentroid.getLon()) / newDocCount;
                        GeoPoint newCentroid = new GeoPoint(newMeanLat, newMeanLon);
                        newCentroids.set(nearestCentroidIndex, newCentroid);
                        mergeMap[i] = nearestCentroidIndex;
                    }
                }
                mergeBuckets(mergeMap, newCentroids.size());
                if (deferringCollector != null) {
                    deferringCollector.mergeBuckets(mergeMap);
                }
                centroids = newCentroids;
                numClusters = newNumClusters;
            }
        }
    }

    private static MinDistanceResult calculateMinDistance(GeoPoint point, ObjectArray<GeoPoint> centroids, long numClusters) {
        int nearestCentroidIndex = -1;
        double minDistance = Double.POSITIVE_INFINITY;
        for (int i = 0; i < numClusters; i++) {
            GeoPoint centroid = centroids.get(i);
            if (centroid != null) {
                double distance = calculateDistance(point, centroid);
                if (distance < minDistance) {
                    nearestCentroidIndex = i;
                    minDistance = distance;
                }
            }
        }
        MinDistanceResult result = new MinDistanceResult();
        result.nearestCentroidIndex = nearestCentroidIndex;
        result.distance = minDistance;
        return result;
    }

    private static double calculateDistance(GeoPoint point1, GeoPoint point2) {
        double latDist = point2.getLat() - point1.getLat();
        double lonDist = point2.getLon() - point1.getLon();
        return Math.sqrt(latDist * latDist + lonDist * lonDist);
    }

    private static class MinDistanceResult {
        public int nearestCentroidIndex;
        public double distance;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        consumeBucketsAndMaybeBreak((int) numClusters);

        if (numClusters == 0) {
            return buildEmptyAggregation();
        }
        mergeClusters();
        long[] bucketOrdArray = new long[(int) numClusters];
        for (int i = 0; i < numClusters; i++) {
            if (centroids.get(i) == null) {
                throw new AssertionError("Tried to store a null centroid in a bucket for centroid at position [" + i
                        + "], centroids size: [" + centroids.size() + "], numClusters: [" + numClusters + "]");
            }
            bucketOrdArray[i] = i;
        }

        runDeferredCollections(bucketOrdArray);

        List<InternalGeoKMeans.Bucket> buckets = new ArrayList<>((int) numClusters);
        for (long i = 0; i < numClusters; i++) {
            buckets.add(new InternalGeoKMeans.Bucket(centroids.get(i), bucketDocCount(i), bucketAggregations(i)));
        }

        return new InternalGeoKMeans(name, numFinalClusters, buckets, numPoints, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoKMeans(name, numFinalClusters, Collections.emptyList(), 0, pipelineAggregators(), metaData());
    }

}

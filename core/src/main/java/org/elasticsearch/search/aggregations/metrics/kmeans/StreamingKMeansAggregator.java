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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.kmeans.GeoKMeans.Cluster;
import org.elasticsearch.search.aggregations.metrics.kmeans.InternalGeoKMeans.InternalCluster;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class StreamingKMeansAggregator extends MetricsAggregator {

    private int numFinalClusters;
    private double maxStreamingClustersCoeff;
    private double distanceCutoffCoeff;
    private double distanceCutoffCoeffMultiplier;

    private ObjectArray<GeoPoint> centroids;
    private LongArray docCounts;
    private ObjectArray<GeoPoint> topLeftClusterBounds;
    private ObjectArray<GeoPoint> bottomRightClusterBounds;
    private int numPoints;
    private Random random;
    private ValuesSource.GeoPoint valuesSource;
    private BigArrays bigArrays;
    private long numClusters;

    public StreamingKMeansAggregator(String name, AggregatorFactories factories, int numClusters, double maxStreamingClustersCoeff,
            double distanceCutoffCoeffMultiplier, ValuesSource.GeoPoint valuesSource, AggregationContext aggregationContext,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, aggregationContext, parent, pipelineAggregators, metaData);
        this.numFinalClusters = numClusters;
        this.maxStreamingClustersCoeff = maxStreamingClustersCoeff;
        this.distanceCutoffCoeffMultiplier = distanceCutoffCoeffMultiplier;
        this.distanceCutoffCoeff = 1;
        this.valuesSource = valuesSource;
        // NOCOMMIT check that is indeed valid to use the shardID as the random
        // seed
        random = new Random(aggregationContext.searchContext().indexShard().shardId().hashCode());
        numPoints = 0;
        bigArrays = aggregationContext.bigArrays();
        centroids = bigArrays.newObjectArray(0);
        docCounts = bigArrays.newLongArray(0);
        topLeftClusterBounds = bigArrays.newObjectArray(0);
        bottomRightClusterBounds = bigArrays.newObjectArray(0);
        numClusters = 0;
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

                GeoPoint previous = null;
                for (int i = 0; i < valuesCount; ++i) {
                    final GeoPoint val = new GeoPoint(values.valueAt(i));
                    collectPoint(val, sub, doc);
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
            centroids = bigArrays.grow(centroids, numClusters + 1);
            docCounts = bigArrays.grow(docCounts, numClusters + 1);
            topLeftClusterBounds = bigArrays.grow(topLeftClusterBounds, numClusters + 1);
            bottomRightClusterBounds = bigArrays.grow(bottomRightClusterBounds, numClusters + 1);
            centroids.set(numClusters, point);
            docCounts.set(numClusters, 1L);
            topLeftClusterBounds.set(numClusters, new GeoPoint(point));
            bottomRightClusterBounds.set(numClusters, new GeoPoint(point));
            numClusters++;
        } else {
            // add to existing centroid
            long newDocCount = docCounts.increment(nearestCentroidIndex, 1L);
            GeoPoint existingClusterTopLeft = topLeftClusterBounds.get(nearestCentroidIndex);
            if (point.getLat() > existingClusterTopLeft.getLat()) {
                existingClusterTopLeft = existingClusterTopLeft.resetLat(point.getLat());
            }
            if (point.getLon() < existingClusterTopLeft.getLon()) {
                existingClusterTopLeft = existingClusterTopLeft.resetLon(point.getLon());
            }
            topLeftClusterBounds.set(nearestCentroidIndex, existingClusterTopLeft);
            GeoPoint existingClusterBottomRight = bottomRightClusterBounds.get(nearestCentroidIndex);

            if (point.getLat() < existingClusterBottomRight.getLat()) {
                existingClusterBottomRight = existingClusterBottomRight.resetLat(point.getLat());
            }
            if (point.getLon() > existingClusterBottomRight.getLon()) {
                existingClusterBottomRight = existingClusterBottomRight.resetLon(point.getLon());
            }
            bottomRightClusterBounds.set(nearestCentroidIndex, existingClusterBottomRight);
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
            ObjectArray<GeoPoint> newCentroids = bigArrays.newObjectArray(1);
            ObjectArray<GeoPoint> newTopLeftClusterBounds = bigArrays.newObjectArray(1);
            ObjectArray<GeoPoint> newBottomRightClusterBounds = bigArrays.newObjectArray(1);
            LongArray newDocCounts = bigArrays.newLongArray(1);
            int newNumClusters = 1;
            newCentroids.set(0, centroids.get(0));
            newTopLeftClusterBounds.set(0, topLeftClusterBounds.get(0));
            newBottomRightClusterBounds.set(0, bottomRightClusterBounds.get(0));
            newDocCounts.set(0, docCounts.get(0));
            for (int i = 1; i < numClusters; i++) {
                GeoPoint centroid = centroids.get(i);
                GeoPoint clusterTopLeft = topLeftClusterBounds.get(i);
                GeoPoint clusterBottomRight = bottomRightClusterBounds.get(i);
                long docCount = docCounts.get(i);
                MinDistanceResult minDistResult = calculateMinDistance(centroid, newCentroids, newNumClusters);
                int nearestCentroidIndex = minDistResult.nearestCentroidIndex;
                double minDistance = minDistResult.distance;
                double f = distanceCutoffCoeff / (numFinalClusters * (1 + Math.log(numPoints)));
                double probability = docCount * minDistance / f;
                if (random.nextDouble() <= probability) {
                    newCentroids = bigArrays.grow(newCentroids, newNumClusters + 1);
                    newDocCounts = bigArrays.grow(newDocCounts, newNumClusters + 1);
                    newTopLeftClusterBounds = bigArrays.grow(newTopLeftClusterBounds, newNumClusters + 1);
                    newBottomRightClusterBounds = bigArrays.grow(newBottomRightClusterBounds, newNumClusters + 1);
                    newCentroids.set(newNumClusters, centroid);
                    newDocCounts.set(newNumClusters, docCount);
                    newTopLeftClusterBounds.set(newNumClusters, clusterTopLeft);
                    newBottomRightClusterBounds.set(newNumClusters, clusterBottomRight);
                    newNumClusters++;
                } else {
                    long newDocCount = newDocCounts.increment(nearestCentroidIndex, docCount);

                    GeoPoint existingClusterTopLeft = newTopLeftClusterBounds.get(nearestCentroidIndex);
                    if (clusterTopLeft.getLat() > existingClusterTopLeft.getLat()) {
                        existingClusterTopLeft = existingClusterTopLeft.resetLat(clusterTopLeft.getLat());
                    }
                    if (clusterTopLeft.getLon() < existingClusterTopLeft.getLon()) {
                        existingClusterTopLeft = existingClusterTopLeft.resetLon(clusterTopLeft.getLon());
                    }
                    newTopLeftClusterBounds.set(nearestCentroidIndex, existingClusterTopLeft);

                    GeoPoint existingClusterBottomRight = newBottomRightClusterBounds.get(nearestCentroidIndex);
                    if (clusterBottomRight.getLat() < existingClusterBottomRight.getLat()) {
                        existingClusterBottomRight = existingClusterBottomRight.resetLat(clusterBottomRight.getLat());
                    }
                    if (clusterBottomRight.getLon() > existingClusterBottomRight.getLon()) {
                        existingClusterBottomRight = existingClusterBottomRight.resetLon(clusterBottomRight.getLon());
                    }
                    newBottomRightClusterBounds.set(nearestCentroidIndex, existingClusterBottomRight);

                    GeoPoint existingCentroid = newCentroids.get(nearestCentroidIndex);
                    double newMeanLat = existingCentroid.getLat()
                            + docCount * (centroid.getLat() - existingCentroid.getLat()) / newDocCount;
                    double newMeanLon = existingCentroid.getLon()
                            + docCount * (centroid.getLon() - existingCentroid.getLon()) / newDocCount;
                    GeoPoint newCentroid = new GeoPoint(newMeanLat, newMeanLon);
                    newCentroids.set(nearestCentroidIndex, newCentroid);
                }
            }
            centroids = newCentroids;
            docCounts = newDocCounts;
            topLeftClusterBounds = newTopLeftClusterBounds;
            bottomRightClusterBounds = newBottomRightClusterBounds;
            numClusters = newNumClusters;
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
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (numClusters == 0) {
            return buildEmptyAggregation();
        }
        mergeClusters();
        List<Cluster> clusters = new ArrayList<>();
        for (int i = 0; i < numClusters; i++) {
            Cluster cluster = new InternalCluster(centroids.get(i), docCounts.get(i), topLeftClusterBounds.get(i),
                    bottomRightClusterBounds.get(i));
            clusters.add(cluster);
        }
        return new InternalGeoKMeans(name, numFinalClusters, clusters, numPoints, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoKMeans(name, numFinalClusters, Collections.emptyList(), 0, pipelineAggregators(), metaData());
    }

}

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
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.geokmeans.InternalGeoKMeans.Bucket;

import java.util.ArrayList;
import java.util.List;

public class StandardKMeans {

    private List<List<Bucket>> combinedClusters;
    private GeoPoint[] centroids;
    private int k;
    private List<Bucket> clusters;

    public StandardKMeans(int k, List<Bucket> clusters) {
        this.k = k;
        this.clusters = clusters;
        combinedClusters = new ArrayList<List<Bucket>>(k);
        centroids = new GeoPoint[k];
        for (int i = 0; i < k; i++) {
            combinedClusters.add(new ArrayList<>());
            combinedClusters.get(i).add(clusters.get(i));
            centroids[i] = clusters.get(i).getCentroid();
        }
    }

    public int nextIteration() {
        assignClusters();
        recalculateCentroids();
        return 0;
    }

    public List<Bucket> getResults(ReduceContext context) {
        List<Bucket> results = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            List<Bucket> subClusters = combinedClusters.get(i);
            GeoPoint centroid = centroids[i];
            long docCount = 0;
            List<InternalAggregations> subAggregations = new ArrayList<>(subClusters.size());
            for (Bucket subCluster : subClusters) {
                docCount += subCluster.getDocCount();
                subAggregations.add((InternalAggregations) subCluster.getAggregations());
            }
            InternalAggregations subAggs = InternalAggregations.reduce(subAggregations, context);
            InternalGeoKMeans.Bucket cluster = new InternalGeoKMeans.Bucket(centroid, docCount, subAggs);
            results.add(cluster);
        }
        return results;
    }

    private void recalculateCentroids() {
        for (int i = 0; i < k; i++) {
            centroids[i] = recalculateCentroid(combinedClusters.get(i));
        }
    }

    private GeoPoint recalculateCentroid(List<Bucket> clusters) {
        GeoPoint newCentroid = new GeoPoint(0.0, 0.0);
        int n = 0;
        for (Bucket cluster : clusters) {
            n++;
            double newLat = newCentroid.lat() + (cluster.getCentroid().lat() - newCentroid.lat()) / n;
            double newLon = newCentroid.lon() + (cluster.getCentroid().lon() - newCentroid.lon()) / n;
            newCentroid = newCentroid.reset(newLat, newLon);
        }
        return newCentroid;
    }

    private void assignClusters() {
        for (Bucket cluster : clusters) {
            int nearestCentroidIndex = calculateMinDistance(cluster.getCentroid(), centroids);
            combinedClusters.get(nearestCentroidIndex).add(cluster);
        }
    }

    private static int calculateMinDistance(GeoPoint point, GeoPoint[] centroids) {
        int nearestCentroidIndex = -1;
        double minDistance = Double.POSITIVE_INFINITY;
        for (int i = 0; i < centroids.length; i++) {
            GeoPoint centroid = centroids[i];
            if (centroid != null) {
                double distance = calculateDistance(point, centroid);
                if (distance < minDistance) {
                    nearestCentroidIndex = i;
                    minDistance = distance;
                }
            }
        }
        return nearestCentroidIndex;
    }

    private static double calculateDistance(GeoPoint point1, GeoPoint point2) {
        double latDist = point2.getLat() - point1.getLat();
        double lonDist = point2.getLon() - point1.getLon();
        return Math.sqrt(latDist * latDist + lonDist * lonDist);
    }
}

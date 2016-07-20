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

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.metrics.kmeans.GeoKMeans.Cluster;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StandardKMeans {

    private Set<Cluster>[] combinedClusters;
    private GeoPoint[] centroids;
    private int k;
    private List<Cluster> clusters;

    public StandardKMeans(int k, List<Cluster> clusters) {
        this.k = k;
        this.clusters = clusters;
        combinedClusters = new HashSet[k];
        centroids = new GeoPoint[k];
        for (int i = 0; i < k; i++) {
            combinedClusters[i] = new HashSet<>();
            combinedClusters[i].add(clusters.get(i));
            centroids[i] = clusters.get(i).getCentroid();
        }
    }

    public int nextIteration() {
        assignClusters();
        recalculateCentroids();
        return 0;
    }

    public List<Cluster> getResults() {
        List<Cluster> results = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            Set<Cluster> subClusters = combinedClusters[i];
            GeoPoint centroid = centroids[i];
            GeoPoint topLeft = null;
            GeoPoint bottomRight = null;
            Set<GeoPoint> points = null;
            long docCount = 0;
            for (Cluster subCluster : subClusters) {
                docCount += subCluster.getDocCount();
                if (subCluster.points() != null) {
                    if (points == null) {
                        points = new HashSet<>();
                    }
                    points.addAll(subCluster.points());
                }
                if (topLeft == null) {
                    topLeft = new GeoPoint(subCluster.getTopLeft());
                } else {
                    if (subCluster.getTopLeft().getLat() > topLeft.getLat()) {
                        topLeft = topLeft.resetLat(subCluster.getTopLeft().getLat());
                    }
                    if (subCluster.getTopLeft().getLon() < topLeft.getLon()) {
                        topLeft = topLeft.resetLon(subCluster.getTopLeft().getLon());
                    }
                }
                if (bottomRight == null) {
                    bottomRight = new GeoPoint(subCluster.getBottomRight());
                } else {
                    if (subCluster.getBottomRight().getLat() < bottomRight.getLat()) {
                        bottomRight = bottomRight.resetLat(subCluster.getBottomRight().getLat());
                    }
                    if (subCluster.getBottomRight().getLon() > bottomRight.getLon()) {
                        bottomRight = bottomRight.resetLon(subCluster.getBottomRight().getLon());
                    }
                }
            }
            InternalGeoKMeans.InternalCluster cluster = new InternalGeoKMeans.InternalCluster(centroid, docCount, topLeft, bottomRight);
            if (points != null) {
                cluster.points(points);
            }
            results.add(cluster);
        }
        return results;
    }

    private void recalculateCentroids() {
        for (int i = 0; i < k; i++) {
            centroids[i] = recalculateCentroid(combinedClusters[i]);
        }
    }

    private GeoPoint recalculateCentroid(Set<Cluster> clusters) {
        GeoPoint newCentroid = new GeoPoint(0.0, 0.0);
        int n = 0;
        for (Cluster cluster : clusters) {
            n++;
            double newLat = newCentroid.lat() + (cluster.getCentroid().lat() - newCentroid.lat()) / n;
            double newLon = newCentroid.lon() + (cluster.getCentroid().lon() - newCentroid.lon()) / n;
            newCentroid = newCentroid.reset(newLat, newLon);
        }
        return newCentroid;
    }

    private void assignClusters() {
        for (Cluster cluster : clusters) {
            int nearestCentroidIndex = calculateMinDistance(cluster.getCentroid(), centroids);
            combinedClusters[nearestCentroidIndex].add(cluster);
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

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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregation;

import java.util.List;
import java.util.Set;

public interface GeoKMeans extends Aggregation {

    List<Cluster> getClusters();

    public interface Cluster extends ToXContent, Writeable {

        GeoPoint getCentroid();

        GeoPoint getTopLeft();

        GeoPoint getBottomRight();

        long getDocCount();

        Set<GeoPoint> points();
    }
}

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

package org.elasticsearch.search.aggregations.metrics.geobounds;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;

public class GeoBoundsAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint, GeoBoundsAggregatorFactory> {

    private final boolean wrapLongitude;

    public GeoBoundsAggregatorFactory(String name, Type type, ValuesSourceConfig<ValuesSource.GeoPoint> config, boolean wrapLongitude,
            AggregationContext context, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        super(name, type, config, context, parent, subFactoriesBuilder);
        this.wrapLongitude = wrapLongitude;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent) throws IOException {
        return new GeoBoundsAggregator(name, context, parent, null, wrapLongitude);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource.GeoPoint valuesSource, Aggregator parent, boolean collectsFromSingleBucket)
            throws IOException {
        return new GeoBoundsAggregator(name, context, parent, valuesSource, wrapLongitude);
    }
}

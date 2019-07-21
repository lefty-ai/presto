/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.combinatoric.presto.elasticsearch;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import io.airlift.units.Duration;

import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.durationProperty;

public class ElasticsearchSessionProperties
{
    private static final String FETCH_SIZE     = "fetch_size";
    private static final String SCROLL_TIMEOUT = "scroll_timeout";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ElasticsearchSessionProperties(ElasticsearchConfig config)
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        FETCH_SIZE,
                        "Number of records to fetch on each request",
                        config.getFetchSize(),
                        false
                ),
                durationProperty(
                        SCROLL_TIMEOUT,
                        "Length of time to keep scroll context alive",
                        config.getScrollTimeout(),
                        false
                )
        );
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static int getFetchSize(ConnectorSession session)
    {
        return session.getProperty(FETCH_SIZE, Integer.class);
    }

    public static Duration getScrollTimeout(ConnectorSession session)
    {
        return session.getProperty(SCROLL_TIMEOUT, Duration.class);
    }
}

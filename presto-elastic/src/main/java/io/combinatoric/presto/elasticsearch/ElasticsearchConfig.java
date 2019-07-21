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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ElasticsearchConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    public static final String DEFAULT_SCHEMA ="default";

    private boolean      https         = false;
    private int          fetchSize     = 1000;
    private Duration     scrollTimeout = new Duration(1000.00, TimeUnit.MILLISECONDS);
    private String       defaultSchema = DEFAULT_SCHEMA;
    private String       username      = null;
    private String       password      = null;
    private List<String> hosts         = ImmutableList.of();

    @Config("elasticsearch.hosts")
    public ElasticsearchConfig setHosts(String commaSeparatedList)
    {
        this.hosts = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    @NotNull
    public List<String> getHosts()
    {
        return hosts;
    }

    @Config("elasticsearch.fetch-size")
    public ElasticsearchConfig setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    @Min(1)
    public int getFetchSize()
    {
        return fetchSize;
    }

    @Config("elasticsearch.scroll-timeout")
    public ElasticsearchConfig setScrollTimeout(Duration scrollTimeout)
    {
        this.scrollTimeout = scrollTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1000ms")
    public Duration getScrollTimeout()
    {
        return scrollTimeout;
    }

    @Config("elasticsearch.default-schema")
    public ElasticsearchConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Config("elasticsearch.https")
    public ElasticsearchConfig setHttps(boolean https)
    {
        this.https = https;
        return this;
    }

    public boolean isHttps()
    {
        return https;
    }

    @Config("elasticsearch.username")
    public ElasticsearchConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("elasticsearch.password")
    public ElasticsearchConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    public Map<String, String> asMap()
    {
        return ImmutableMap.<String, String>builder()
                .put("elasticsearch.hosts", hosts.stream().collect(Collectors.joining(",")))
                .put("elasticsearch.fetch-size", String.valueOf(fetchSize))
                .put("elasticsearch.scroll-timeout", String.valueOf(scrollTimeout))
                .put("elasticsearch.default-schema", defaultSchema)
                .build();
    }
}

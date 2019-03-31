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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.List;

public class ElasticsearchClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<String> clusterHosts = ImmutableList.of();

    @Config("elasticsearch.cluster-hosts")
    public ElasticsearchClientConfig setClusterHosts(String commaSeparatedList)
    {
        this.clusterHosts = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    @NotNull
    @Size(min = 1)
    public List<String> getClusterHosts()
    {
        return clusterHosts;
    }
}

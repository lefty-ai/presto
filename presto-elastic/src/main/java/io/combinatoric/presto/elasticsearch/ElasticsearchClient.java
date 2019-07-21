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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;

import io.prestosql.spi.PrestoException;

import io.airlift.log.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.*;

import io.combinatoric.presto.elasticsearch.shard.ShardInfo;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static io.combinatoric.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_METADATA_ERROR;
import static io.combinatoric.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CONNECTION_ERROR;

public class ElasticsearchClient implements Closeable
{
    private static final Logger log = Logger.get(ElasticsearchClient.class);

    private final RestHighLevelClient     client;
    private final ImmutableList<HttpHost> hosts;

    public ElasticsearchClient(ElasticsearchConfig config)
    {
        requireNonNull(config, "config is null");

        ImmutableList.Builder<HttpHost> builder = ImmutableList.builder();

        for (String clusterHost : config.getHosts())
        {
            HostAndPort hostAndPort = HostAndPort.fromString(clusterHost).withDefaultPort(9200);
            HttpHost host = new HttpHost(hostAndPort.getHost(), hostAndPort.getPort(), config.isHttps() ? "https" : "http");
            log.info(String.format("Elasticsearch client connecting to: [%s] [https: %s]", host.toString(), config.isHttps()));

            builder.add(host);
        }

        hosts = builder.build();

        if (config.isHttps() && (config.getUsername() != null && config.getPassword() != null))
        {
            final CredentialsProvider provider = new BasicCredentialsProvider();
            provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));

            client = new RestHighLevelClient(RestClient.builder(hosts.toArray(new HttpHost[]{}))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback()
                    {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder)
                        {
                            return httpClientBuilder.setDefaultCredentialsProvider(provider);
                        }
                    }));
        }
        else {
            client = new RestHighLevelClient(RestClient.builder(hosts.toArray(new HttpHost[] {})));
        }
    }

    public ShardInfo shardInfo(String index)
    {
        JsonNode root = null;

        try {
            RestClient _client = client.getLowLevelClient();
            Request request = new Request("GET", format("/%s/_shard_stores", index));
            request.addParameter("status", "green");

            Response response = _client.performRequest(request);

            if (HttpStatus.SC_OK != response.getStatusLine().getStatusCode()) {
                throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format(
                        "Error reading shard store info for [%s]; status code: %d", index, response.getStatusLine().getStatusCode()));
            }

            String body = EntityUtils.toString(response.getEntity());
            root = new ObjectMapper().readTree(body);
        }
        catch (IOException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format("Error reading shard store info for [%s]", index, e));
        }

        JsonNode indices = root.get("indices");
        JsonNode idx     = indices.get(index);

        if (idx == null) {
            throw new PrestoException(ELASTICSEARCH_METADATA_ERROR, format("Failed to read shard store info for [%s]", index));
        }

        ShardInfo si = new ShardInfo(index);

        Iterator<Map.Entry<String, JsonNode>> shards = idx.get("shards").fields();
        while (shards.hasNext())
        {
            Map.Entry<String, JsonNode> next = shards.next();

            String shardId              = next.getKey();
            JsonNode stores             = next.getValue().get("stores");
            Iterator<JsonNode> elements = stores.elements();

            ImmutableList.Builder<ShardInfo.ShardStoreNode> builder = ImmutableList.builder();

            while (elements.hasNext())
            {
                JsonNode store = elements.next();
                Map.Entry<String, JsonNode> storeTopLevel = store.fields().next();
                JsonNode storeTopLevelNode = storeTopLevel.getValue();

                String storeNodeName    = storeTopLevelNode.get("name").textValue();
                String transportAddress = storeTopLevelNode.get("transport_address").textValue();
                JsonNode allocation     = store.findValue("allocation");

                builder.add(new ShardInfo.ShardStoreNode(storeNodeName, transportAddress, allocation.textValue()));
            }

            si.stores().put(Integer.parseInt(shardId), new ShardInfo.ShardStore(shardId, builder.build()));
        }

        return si;
    }

    public RestHighLevelClient client()
    {
        return client;
    }

    public List<HttpHost> hosts()
    {
        return hosts;
    }

    @Override
    public void close() throws IOException
    {
        client.close();
    }
}

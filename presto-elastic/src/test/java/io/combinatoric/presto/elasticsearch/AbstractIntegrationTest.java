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

import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.testing.TestingConnectorContext;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import io.airlift.log.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.concurrent.TimeUnit.MINUTES;


public abstract class AbstractIntegrationTest
{
    private static final Logger log = Logger.get(AbstractIntegrationTest.class);
    private static final int NUM_DATA_NODES = 2;
    private static final String ELASTICSEARCH_VERSION = "6.5.4";
    private static final String CLUSTER_NAME = "embedded-elasticsearch";

    private static Connector connector;
    private static EmbeddedElastic[] cluster;
    protected static ElasticsearchClient client;
    protected static ConnectorMetadata metadata;

    @BeforeClass
    public void beforeClass()
            throws IOException, InterruptedException
    {
        cluster = spinUpElasticsearchCluster(NUM_DATA_NODES);
        connector = initializeConnector();
        metadata = connector.getMetadata(new ConnectorTransactionHandle() {});

        /*
        ElasticsearchClientConfig config = new ElasticsearchClientConfig();

        StringJoiner joiner = new StringJoiner(",");
        for (EmbeddedElastic elastic : cluster) {
            joiner.add("localhost:" + elastic.getHttpPort());
        }

        config.setClusterHosts(joiner.toString());

        client = new ElasticsearchClient(config);
        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse health = client.client().cluster().health(request, RequestOptions.DEFAULT);
        assertEquals(health.getNumberOfDataNodes(), NUM_DATA_NODES);
        */
    }

    private Connector initializeConnector()
    {
        ElasticsearchPlugin plugin = new ElasticsearchPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, ElasticsearchConnectorFactory.class);

        StringJoiner joiner = new StringJoiner(",");
        for (EmbeddedElastic elastic : cluster) {
            joiner.add("localhost:" + elastic.getHttpPort());
        }

        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("elasticsearch.cluster-hosts",  joiner.toString())
                .build();

        return factory.create("test-connector", config, new TestingConnectorContext());
    }

    private EmbeddedElastic[] spinUpElasticsearchCluster(int nodes)
            throws IOException, InterruptedException
    {
        EmbeddedElastic[] elastic = new EmbeddedElastic[nodes];

        for (int i = 0; i < nodes; i++) {
            String name = "es-node-" + i;
            log.info("Starting embedded elasticsearch node: " + name);

            EmbeddedElastic.Builder builder = EmbeddedElastic.builder()
                    .withElasticVersion(ELASTICSEARCH_VERSION)
                    .withSetting(PopularProperties.TRANSPORT_TCP_PORT, (9500 + i))
                    .withSetting(PopularProperties.HTTP_PORT, 9400 + i)
                    .withSetting(PopularProperties.CLUSTER_NAME, "embedded-elasticsearch")
                    .withSetting("node.name", name)
                    .withEsJavaOpts("-Xms512m -Xmx512m")
                    .withStartTimeout(1, MINUTES)
                    .withInstallationDirectory(Files.createTempDir());

            if (i > 0) {
                builder.withSetting("discovery.zen.ping.unicast.hosts", "localhost:" + 9500);
            }

            elastic[i] = builder.build().start();
        }

        return elastic;
    }

    @AfterClass
    public void afterClass()
    {
        try {
            client.close();
        }
        catch (IOException e) {
            log.error(e);
        }

        for (EmbeddedElastic elastic : cluster) {
            try {
                elastic.stop();
            }
            catch (Exception e) {
                log.error(e);
            }
        }
    }
}

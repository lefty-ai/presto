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

import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.*;
import io.prestosql.testing.TestingConnectorContext;

import io.airlift.log.Logger;

import com.google.common.io.Files;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import java.io.IOException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;

import io.combinatoric.presto.elasticsearch.TableUtil.TestTable;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.combinatoric.presto.elasticsearch.type.Types.toElasticsearchTypeName;
import static java.util.concurrent.TimeUnit.MINUTES;


public abstract class IntegrationTestBase extends RandomizedTest
{
    private static final Logger logger = Logger.get(IntegrationTestBase.class);

    private static final int    NUM_DATA_NODES        = 1;
    private static final String ELASTICSEARCH_VERSION = "6.7.0";
    private static final String CLUSTER_NAME          = "embedded-elasticsearch";

    private   static Connector           connector;
    private   static EmbeddedElastic[]   cluster;
    protected static ElasticsearchClient client;
    protected static ConnectorMetadata   metadata;
    protected static ElasticsearchConfig config;

    @BeforeClass
    public static void beforeClass() throws IOException, InterruptedException
    {
        ElasticsearchPlugin plugin = loadPlugin(ElasticsearchPlugin.class);
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, ElasticsearchConnectorFactory.class);

        String hosts = System.getProperty("elasticsearch.hosts");
        if (hosts != null) {
            config = new ElasticsearchConfig().setHosts(hosts);
        }
        else {
            cluster = spinUpElasticsearchCluster(NUM_DATA_NODES);
            String endpoints = Stream.of(cluster)
                    .map(es -> "localhost:" + es.getHttpPort()).collect(Collectors.joining(","));
            config = new ElasticsearchConfig().setHosts(endpoints);
        }

        connector = factory.create("test-connector", config.asMap(), new TestingConnectorContext());
        client    = new ElasticsearchClient(config);
        metadata  = connector.getMetadata(new ConnectorTransactionHandle() {});
    }

    private static EmbeddedElastic[] spinUpElasticsearchCluster(int nodes) throws IOException, InterruptedException
    {
        EmbeddedElastic[] elastic = new EmbeddedElastic[nodes];

        for (int i = 0; i < nodes; i++) {
            String name = "es-node-" + i;
            logger.info("Starting embedded elasticsearch node: " + name);

            EmbeddedElastic.Builder builder = EmbeddedElastic.builder()
                    .withElasticVersion(ELASTICSEARCH_VERSION)
                    .withSetting(PopularProperties.TRANSPORT_TCP_PORT, (9500 + i))
                    .withSetting(PopularProperties.HTTP_PORT, 9400 + i)
                    .withSetting(PopularProperties.CLUSTER_NAME, CLUSTER_NAME)
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

    /**
     * Creates an empty schema with the given name.
     */
    protected void createIndex(String index) throws IOException
    {
        client.client().indices().create(
                new CreateIndexRequest(index).settings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0).build()).waitForActiveShards(ActiveShardCount.ALL),
                RequestOptions.DEFAULT);
    }

    /**
     * Deletes the given index.
     */
    protected void dropIndex(String index) throws IOException
    {
        boolean exists = client.client().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
        if (!exists) {
            return;
        }

        AcknowledgedResponse response = client.client().indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
        if (!response.isAcknowledged()) {
            throw new RuntimeException("Failed receive acknowledgement for index deletion: [" + index + "]");
        }
    }

    protected void dropTable(TestTable table) throws IOException
    {
        dropIndex(table.schema().getTableName());
    }

    protected TestTable createTable(TestTable table) throws IOException
    {
        mapping(table.schema(), table.columns());
        generate(table.schema(), table.columns());
        return table;
    }

    /**
     * Creates an index mapping for the given columns.
     */
    protected void mapping(SchemaTableName st, List<ElasticsearchColumnHandle> columns) throws IOException
    {
        XContentBuilder json = XContentFactory.jsonBuilder();

        json.startObject();
        json.startObject("properties");
        _mapping(json, columns);
        json.endObject();
        json.endObject();

        logger.debug("mapping:\n" + Strings.toString(json));

        client.client().indices().create(
                new CreateIndexRequest(st.getTableName())
                        .mapping(json)
                        .settings(Settings.builder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0).build())
                        .waitForActiveShards(ActiveShardCount.ALL),
                RequestOptions.DEFAULT);

    }

    private void _mapping(XContentBuilder json, List<ElasticsearchColumnHandle> columns) throws IOException
    {
        for (ElasticsearchColumnHandle column : columns) {
            String typename = toElasticsearchTypeName(column.getColumnType());
            json.startObject(column.getColumnName());

            switch (typename) {
                default:
                    json.field("type", typename);
            }

            json.endObject();
        }
    }

    private void generate(SchemaTableName table, List<ElasticsearchColumnHandle> columns) throws IOException
    {
        XContentBuilder json = XContentFactory.jsonBuilder();
        json.startObject();

        for (ElasticsearchColumnHandle column : columns) {
            switch (column.getColumnType().getTypeSignature().getBase())
            {
                case "integer":
                    json.field(column.getColumnName(), randomInt());
                    break;
                case "bigint":
                    json.field(column.getColumnName(), randomLong());
                    break;
                case "tinyint":
                    json.field(column.getColumnName(), randomByte());
                    break;
                case "smallint":
                    json.field(column.getColumnName(), randomShort());
                    break;
                case "boolean":
                    json.field(column.getColumnName(), randomBoolean());
                    break;
                case "varchar":
                    json.field(column.getColumnName(), randomAsciiAlphanumOfLengthBetween(0, 1024));
                    break;
                default:
                    throw new RuntimeException("Unsupported data type: " + column.getColumnType().getTypeSignature().getBase());
            }
        }

        json.endObject();

        IndexResponse response = client.client().index(new IndexRequest(table.getTableName()).source(json), RequestOptions.DEFAULT);
        if (response.status() != RestStatus.CREATED) {
            throw new RuntimeException("Unable to index data: " + response.status());
        }
    }

    @AfterClass
    public static void afterClass()
    {
        try {
            client.close();
        }
        catch (IOException e) {
            logger.error(e);
        }

        if (cluster != null) {
            for (EmbeddedElastic elastic : cluster) {
                try {
                    elastic.stop();
                }
                catch (Exception e) {
                    logger.error(e);
                }
            }
        }
    }

    /**
     * Load plugin from service loader.
     */
    @SuppressWarnings("unchecked")
    private static <T extends Plugin> T loadPlugin(Class<T> clazz)
    {
        for (Plugin plugin : ServiceLoader.load(Plugin.class)) {
            if (clazz.isInstance(plugin)) {
                return (T) plugin;
            }
        }
        throw new AssertionError("unable to load plugin: " + clazz.getName());
    }
}

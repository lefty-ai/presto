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
import io.prestosql.testing.TestingConnectorContext;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import org.junit.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNotNull;

public class TestElasticsearchPlugin
{
    @Test
    public void testCreateConnector()
    {
        ElasticsearchPlugin plugin = new ElasticsearchPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, ElasticsearchConnectorFactory.class);

        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("elasticsearch.cluster-hosts", "localhost:9200")
                .build();

        Connector connector = factory.create("test-connector", config, new TestingConnectorContext());
        assertNotNull(connector);
    }
}

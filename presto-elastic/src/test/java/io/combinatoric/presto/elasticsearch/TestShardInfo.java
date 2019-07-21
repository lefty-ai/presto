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

import io.airlift.log.Logger;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.util.Map;

import io.combinatoric.presto.elasticsearch.shard.ShardInfo;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TestShardInfo extends IntegrationTestBase
{
    private static final Logger logger = Logger.get(TestElasticsearchPageSource.class);

    @Test
    public void testShardInfo() throws Exception
    {
        int totalShards = 3;
        TableUtil.TestTable table = create(TableUtil.TestTable.ofAll(), 3, totalShards);

        try {
            ShardInfo shardInfo = client.shardInfo(table.schema().getTableName());

            assertThat(shardInfo.shardCount(), equalTo(totalShards));

            for (Map.Entry<Integer, ShardInfo.ShardStore> entry : shardInfo.stores().entrySet()) {
                logger.info("shard id: " + entry.getKey());
                logger.info("shard store: " + entry.getValue());
            }
        }
        finally {
            drop(table);
        }
    }
}

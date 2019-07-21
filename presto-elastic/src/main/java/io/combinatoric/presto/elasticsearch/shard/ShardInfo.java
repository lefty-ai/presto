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
package io.combinatoric.presto.elasticsearch.shard;

import com.google.common.base.MoreObjects;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ShardInfo
{
    private final String index;

    private Map<Integer, ShardStore> stores;

    public ShardInfo(String index)
    {
        this.index = index;
        this.stores = new HashMap<>();
    }

    public Map<Integer, ShardStore> stores()
    {
        return stores;
    }

    public int shardCount()
    {
        return stores.size();
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("index", index)
                .add("stores", stores)
                .toString();
    }

    public static final class ShardStore
    {
        private final String               shardId;
        private final List<ShardStoreNode> storeNodes;

        public ShardStore(String shardId, List<ShardStoreNode> storeNodes)
        {
            this.shardId = shardId;
            this.storeNodes = storeNodes;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("shardId", shardId)
                    .add("storeNodes", storeNodes)
                    .toString();
        }
    }

    public static final class ShardStoreNode
    {
        private final String name;
        private final String address;
        private final String type;

        public ShardStoreNode(String name, String address, String type)
        {
            this.name    = name;
            this.address = address;
            this.type    = type;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("name", name)
                    .add("address", address)
                    .add("type", type)
                    .toString();
        }
    }
}



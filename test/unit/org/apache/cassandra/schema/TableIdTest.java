/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.schema;

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class TableIdTest
{
    @Test
    public void testTableNameAndIdFromFilename()
    {
        TableId tableId = TableId.generate();
        Pair<String, TableId> tableNameAndId = TableId.tableNameAndIdFromFilename("cf-" + tableId.toHexString());
        assertNotNull(tableNameAndId);
        assertEquals("cf", tableNameAndId.left);
        assertEquals(tableId, tableNameAndId.right);
    }

    @Test
    public void testToStringIsCached()
    {
        UUID uuid = UUID.randomUUID();
        TableId tableId = TableId.fromUUID(uuid);
        String first = tableId.toString();
        assertEquals(uuid.toString(), first);
        assertSame(first, tableId.toString());
        assertEquals(TableId.fromString(first), tableId);
    }
}
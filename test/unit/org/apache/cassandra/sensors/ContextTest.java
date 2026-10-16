/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sensors;

import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ContextTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testFromTableMetadata()
    {
        UUID uuid = UUID.randomUUID();
        TableMetadata metadata = TableMetadata.builder("ks", "tbl", TableId.fromUUID(uuid))
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .build();

        Context expected = new Context("ks", "tbl", uuid.toString());
        Context context = Context.from(metadata);
        assertEquals(expected, context);
        assertEquals(expected.hashCode(), context.hashCode());

        // The table id string is computed once per table, not once per context
        assertSame(context.getTableId(), Context.from(metadata).getTableId());
        assertSame(context.getTableId(), Context.from(metadata.unbuild().build()).getTableId());
    }
}

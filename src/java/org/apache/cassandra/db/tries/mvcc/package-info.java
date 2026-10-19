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

/// Storage for cell value versions kept outside memtable memory. The memtable cell holds a pointer to the current
/// version of its value; the bytes themselves live in a [ValueStore], which hands out opaque handles. Stores are
/// append-only: a value is never modified or freed individually, and all of a store's storage is reclaimed at once
/// when the memtable that owns it is discarded.
///
/// The package depends on `db.commitlog` only for `CommitLogValueLocations`: where a write put its values in the
/// commit log, which [ValueStore#append] may refer to instead of copying them.
package org.apache.cassandra.db.tries.mvcc;

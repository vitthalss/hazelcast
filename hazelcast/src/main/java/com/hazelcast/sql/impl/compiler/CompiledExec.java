/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.compiler;

import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayList;

/**
 * Parent class for compiled executors.
 */
public abstract class CompiledExec extends AbstractExec {
    // TODO: Need to externalize it somehow.
    private static final int BATCH_SIZE = 1024;

    /** Subsumed IDs. */
    private final int[] subsumedIds;

    /** Current row. */
    private ArrayList<Row> currentBatch;

    protected CompiledExec(int id, int[] subsumedIds) {
        super(id);

        this.subsumedIds = subsumedIds;
    }

    public abstract void prepare(Exec exec);

    protected final void clearBatch() {
        if (currentBatch != null) {
            currentBatch = null;
        }
    }

    protected final boolean addToBatch(HeapRow row) {
        if (currentBatch == null) {
            currentBatch = new ArrayList<>(BATCH_SIZE);
        }

        currentBatch.add(row);

        return currentBatch.size() <= BATCH_SIZE;
    }

    @Override
    protected RowBatch currentBatch0() {
        return currentBatch != null ? new ListRowBatch(currentBatch) : null;
    }

    @Override
    public boolean canReset() {
        return false;
    }

    public int[] getSubsumedIds() {
        return subsumedIds;
    }
}

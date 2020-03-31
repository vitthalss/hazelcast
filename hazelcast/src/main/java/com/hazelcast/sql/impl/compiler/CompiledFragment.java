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

import com.hazelcast.sql.impl.exec.Exec;

import java.util.Map;

/**
 * Compiled query fragment.
 */
public class CompiledFragment {
    /** Top-level executors which should be used for replacement. */
    private final Map<Integer, CompiledExec> topExecs;

    /** Subsumed executors. */
    private final Map<Integer, CompiledExec> subsumedExecs;

    public CompiledFragment(Map<Integer, CompiledExec> topExecs, Map<Integer, CompiledExec> subsumedExecs) {
        this.topExecs = topExecs;
        this.subsumedExecs = subsumedExecs;
    }

    /**
     * Get's compiled executor which should be used instead of the one with the given ID.
     *
     * @param execId ID of the executor to be replaced.
     * @return Replacement or {@code null} if original executor should not be replaced.
     */
    public CompiledExec getExecutor(int execId) {
        return topExecs.get(execId);
    }

    /**
     * Prepare the compiled fragment with the given executor.
     */
    public void prepare(Exec exec) {
        if (exec instanceof CompiledExec) {
            // Compiled fragments are never concerned with each other's state, because they should form a single fragment.
            return;
        }

        CompiledExec compiledExec = subsumedExecs.get(exec.getId());

        if (compiledExec != null) {
            compiledExec.prepare(exec);
        }
    }
}

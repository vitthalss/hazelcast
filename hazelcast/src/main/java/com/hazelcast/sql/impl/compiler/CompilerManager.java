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

import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.physical.PhysicalNode;

/**
 * Manager which handles query compilation.
 */
public class CompilerManager {
    // TODO: Remove this flag (now needed only fo tests).
    @SuppressWarnings({"checkstyle:StaticVariableName", "checkstyle:VisibilityModifier"})
    public static volatile boolean TEST_ENABLED;

    /** Optimizer. */
    private final SqlOptimizer optimizer;

    /** Whether query compilation is enabled. */
    private final boolean enabled;

    public CompilerManager(SqlOptimizer optimizer) {
        this.optimizer = optimizer;

        enabled = optimizer.canCompile();
    }

    public CompiledFragmentTemplate getTemplate(PhysicalNode node) {
        if (!enabled) {
            return null;
        }

        return getTemplate0(node);
    }

    private CompiledFragmentTemplate getTemplate0(PhysicalNode node) {
        // TODO:
        //  1. Decide whether to compile or not based on heuristics
        //  2. Start asynchronous compilation.
        //  3. Return from cache when ready.
        return TEST_ENABLED ? optimizer.compile(node) : null;
    }
}

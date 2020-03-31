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

import com.hazelcast.sql.impl.plan.node.PlanNode;

/**
 * Result of physical node compilation.
 */
public class CompilerResult {
    /** Node which triggered the compilation. */
    private final PlanNode node;

    /** Compiled class. */
    private final Class<? extends CompiledExec> clazz;

    /** Compiled class source (for debugging purposes). */
    private final String source;

    public CompilerResult(PlanNode node, Class<? extends CompiledExec> clazz, String source) {
        this.node = node;
        this.clazz = clazz;
        this.source = source;
    }

    public PlanNode getNode() {
        return node;
    }

    public Class<? extends CompiledExec> getClazz() {
        return clazz;
    }

    public String getSource() {
        return source;
    }
}

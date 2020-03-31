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

package com.hazelcast.sql.impl.compiler.exec;

import com.hazelcast.sql.impl.compiler.LocalVariable;
import com.hazelcast.sql.impl.compiler.SqlCompiler;
import com.hazelcast.sql.impl.compiler.emitter.EmitableMethod;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.plan.node.PlanNode;

import java.util.List;

/**
 * Node which cannot be compiled.
 */
public class UnsupportedCodeGenerator extends CodeGenerator<PlanNode> {
    public UnsupportedCodeGenerator(PlanNode node) {
        super(node);
    }

    @Override
    public void prepare(SqlCompiler compiler) {
        // TODO: Register itself as upstream
    }

    @Override
    public List<LocalVariable> advanceStart(EmitableMethod method, List<List<LocalVariable>> inputs) {
        // TODO: Get upstream row
        return null;
    }

    @Override
    public void advanceFinish(EmitableMethod method) {
        // TODO: Implement me.
    }

    @Override
    public boolean canCompile() {
        return false;
    }

    @Override
    public Class<? extends Exec> getExecClass() {
        return Exec.class;
    }

    @Override
    public boolean isTerminal() {
        throw new IllegalStateException("Should not be called on non-compilable nodes.");
    }
}

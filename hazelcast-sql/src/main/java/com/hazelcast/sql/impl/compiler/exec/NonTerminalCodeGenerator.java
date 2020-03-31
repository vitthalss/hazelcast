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

import java.util.Collections;
import java.util.List;

import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_COMPILED_EXEC__M_ADD_TO_BATCH;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_ITERATION_RESULT__F_FETCHED;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.fieldStatic;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.invoke;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_EXCLAMATION;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_IF;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_RETURN;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SEMICOLON;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_THIS;

/**
 * Code generator for non-terminal node. Ensures that constructed row data is collected into a row batch, so that it could be
 * consumed by downstream nodes.
 * This node is only needed for cases when local parent operator exists, but cannot be compiled.
 */
public class NonTerminalCodeGenerator extends CodeGenerator<PlanNode> {
    public NonTerminalCodeGenerator(CodeGenerator<?> child) {
        super(child.getNode(), Collections.singletonList(child));
    }

    @Override
    public void prepare(SqlCompiler compiler) {
        // No-op.
    }

    @Override
    public List<LocalVariable> advanceStart(EmitableMethod method, List<List<LocalVariable>> inputs) {
        assert inputs.size() == 1;

        // Register the row.
        LocalVariable row = method.registerRowLocalVariable(getNodeId(), null, inputs.get(0));

        // Add to the batch and return if there is no more space.
        method.addNewLine();

        method
            .addContent(C_IF, C_SPACE, C_L_PAR, C_EXCLAMATION)
            .add(invoke(C_THIS, C_COMPILED_EXEC__M_ADD_TO_BATCH, row.getName()))
            .addContent(C_R_PAR, C_SPACE, C_L_BRACE)
            .addTab()
            .addNewLine();

        method
            .addContent(C_RETURN, C_SPACE)
            .add(fieldStatic(method, C_ITERATION_RESULT__F_FETCHED))
            .addContent(C_SEMICOLON)
            .removeTab()
            .addNewLine();

        method
            .addContent(C_R_BRACE);

        return null;
    }

    @Override
    public void advanceFinish(EmitableMethod method) {
        // No-op.
    }

    @Override
    public Class<? extends Exec> getExecClass() {
        return children.get(0).getExecClass();
    }

    @Override
    public boolean isTerminal() {
        return true;
    }
}

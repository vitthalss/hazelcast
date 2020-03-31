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

package com.hazelcast.sql.impl.compiler.expression;

import com.hazelcast.sql.impl.compiler.LocalVariable;
import com.hazelcast.sql.impl.compiler.emitter.EmitableMethod;
import com.hazelcast.sql.impl.compiler.emitter.EmitterCommand;
import com.hazelcast.sql.impl.expression.Expression;

import java.util.List;

import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_EXPRESSION__M_EVAL;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.invoke;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_EQUALS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SEMICOLON;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_UNDERSCORE;

public final class ExpressionCompiler {
    /** Singleton instance. */
    public static final ExpressionCompiler INSTANCE = new ExpressionCompiler();

    private ExpressionCompiler() {
        // No-op.
    }

    public LocalVariable compile(
        EmitableMethod method,
        List<LocalVariable> row,
        Expression<?> expression,
        String expressionTarget,
        int nodeId,
        String suffix
    ) {
        // Optimistic path: expression can be compiled.
        String varPrefix = C_UNDERSCORE + nodeId + C_UNDERSCORE + suffix + C_UNDERSCORE;

        LocalVariable res = tryCompile(method, row, expression, varPrefix);

        if (res != null) {
            return res;
        }

        // Pessimistic path: expression cannot be compiled, so evaluate it.
        return compileInterpreted(method, row, expressionTarget, nodeId, suffix);
    }

    private LocalVariable tryCompile(
        EmitableMethod method,
        List<LocalVariable> row,
        Expression<?> expression,
        String varPrefix
    ) {
        try {
            // Build commands.
            ExpressionCompilerVisitor visitor = new ExpressionCompilerVisitor(method, row, varPrefix);

            LocalVariable res = expression.visit(visitor);

            // Submit commands to the method after compilation is proven to be possible.
            for (EmitterCommand command : visitor.getCommands()) {
                method.add(command);
                method.addNewLine();
            }

            return res;
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    private LocalVariable compileInterpreted(
        EmitableMethod method,
        List<LocalVariable> row,
        String expressionTarget,
        int nodeId,
        String suffix
    ) {
        // Create the row if needed.
        LocalVariable rowVar = method.registerRowLocalVariable(nodeId, suffix, row);

        // Evaluate the expression.
        String expVarName = C_UNDERSCORE + nodeId + C_UNDERSCORE + suffix;
        LocalVariable expVar = method.addLocalVariable(expVarName, Object.class);

        // TODO: We pass null to eval() as the second argument at the moment. Fix that.
        method.addContent(expVar.getClassName(), C_SPACE, expVar.getName(), C_SPACE, C_EQUALS, C_SPACE)
            .add(invoke(expressionTarget, C_EXPRESSION__M_EVAL, rowVar.getName(), "null")).addContent(C_SEMICOLON).addNewLine();

        return expVar;
    }
}

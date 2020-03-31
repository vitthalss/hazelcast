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
import com.hazelcast.sql.impl.compiler.emitter.EmitterCommands;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ExpressionVisitor;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_EQUALS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_PLUS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SEMICOLON;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_UNDERSCORE;

/**
 * Visitor performing expression compilation.
 */
public class ExpressionCompilerVisitor implements ExpressionVisitor<LocalVariable> {
    /** Method. */
    private final EmitableMethod method;

    /** Row. */
    private final List<LocalVariable> row;

    /** Prefix which is applied to new local variables. */
    private final String varPrefix;

    /** Counter for local variables. */
    private int varCounter;

    /** Emitted commands. */
    private final List<EmitterCommand> commands = new ArrayList<>();

    public ExpressionCompilerVisitor(EmitableMethod method, List<LocalVariable> row, String varPrefix) {
        this.method = method;
        this.row = row;
        this.varPrefix = varPrefix;
    }

    @Override
    public LocalVariable visitColumn(ColumnExpression<?> expression) {
        // Column compilation is straightforward: just get the already available local variable.
        return row.get(expression.getIndex());
    }

    @Override
    public LocalVariable visitPlusFunction(PlusFunction<?> expression, LocalVariable operand1, LocalVariable operand2) {
        if (expression.getType().getTypeFamily() == QueryDataTypeFamily.INT) {
            LocalVariable res = method.addLocalVariable(nextVariableName(), Integer.class);

            // TODO: Null checks
            commands.add(EmitterCommands.content(res.getClassName(), C_SPACE, res.getName(), C_SPACE, C_EQUALS, C_SPACE,
                operand1.getName(), C_SPACE, C_PLUS, C_SPACE, operand2.getName(), C_SEMICOLON));

            return res;
        }

        // TODO: Implement me.
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalVariable visitComparisonPredicate(ComparisonPredicate predicate, LocalVariable operand1, LocalVariable operand2) {
        if (predicate.getComparisonMode() == ComparisonMode.EQUALS) {
            LocalVariable res = method.addLocalVariable(nextVariableName(), Boolean.class);

            // TODO: Make sure to apply casts here. Essentially, we need to emit converter's code.
            commands.add(EmitterCommands.content(res.getClassName(), C_SPACE, res.getName(), C_SPACE, C_EQUALS, C_SPACE,
                "(", operand1.getName(), " == null || ", operand2.getName(), " == null) ? null : Integer.compare(",
                operand1.getName(), ", ", operand2.getName(), ") == 0;"));

            return res;
        }

        // TODO: Implement me.
        throw new UnsupportedOperationException();
    }

    public List<EmitterCommand> getCommands() {
        return commands;
    }

    public String nextVariableName() {
        return varPrefix + C_UNDERSCORE + varCounter++;
    }
}

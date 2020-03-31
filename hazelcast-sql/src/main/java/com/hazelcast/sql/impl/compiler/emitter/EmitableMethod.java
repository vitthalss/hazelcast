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

package com.hazelcast.sql.impl.compiler.emitter;

import com.hazelcast.sql.impl.compiler.CompilerUtils;
import com.hazelcast.sql.impl.compiler.CompilerVisibilityModifier;
import com.hazelcast.sql.impl.compiler.LocalVariable;
import com.hazelcast.sql.impl.row.HeapRow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_HEAP_ROW__M_OF;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.invokeStatic;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterCommands.content;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_COMMA;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_EQUALS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SEMICOLON;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_UNDERSCORE;

public class EmitableMethod implements Emitable {

    public static final String CTOR = "ctor";

    private final EmitableClass parent;
    private final CompilerVisibilityModifier modifier;
    private final String name;
    private final String returnType;
    private final List<EmitableMethodArgument> arguments = new ArrayList<>();
    private final List<EmitterCommand> commands = new ArrayList<>();
    private final Map<String, LocalVariable> localVariables = new HashMap<>();

    public EmitableMethod(EmitableClass parent, CompilerVisibilityModifier modifier, String name, String returnType) {
        this.modifier = modifier;
        this.parent = parent;
        this.name = name;
        this.returnType = returnType;
    }

    public EmitableClass getParent() {
        return parent;
    }

    public EmitableMethod addArgument(String name, Class<?> clazz) {
        for (EmitableMethodArgument argument : arguments) {
            if (argument.getName().equals(name)) {
                throw new IllegalArgumentException("Argument already defined: " + name);
            }
        }

        parent.addImport(clazz);

        arguments.add(new EmitableMethodArgument(name, CompilerUtils.toSimpleName(clazz)));

        return this;
    }

    public EmitableMethod add(EmitterCommand command) {
        this.commands.add(command);

        return this;
    }

    public EmitableMethod addContent(Object... data) {
        commands.add(content(data));

        return this;
    }

    public EmitableMethod addNewLine() {
        commands.add(EmitterCommands.newLine());

        return this;
    }

    public EmitableMethod addTab() {
        commands.add(EmitterCommands.addTab());

        return this;
    }

    public EmitableMethod removeTab() {
        commands.add(EmitterCommands.removeTab());

        return this;
    }

    public LocalVariable addLocalVariable(String name, String className) {
        LocalVariable res = new LocalVariable(name, className);

        if (localVariables.putIfAbsent(name, res) != null) {
            throw new IllegalStateException("Variable already registered: " + name);
        }

        return res;
    }

    public LocalVariable addLocalVariable(String name, Class<?> clazz) {
        parent.addImport(clazz);

        return addLocalVariable(name, CompilerUtils.toSimpleName(clazz));
    }

    public LocalVariable getLocalVariable(String name) {
        return localVariables.get(name);
    }

    // TODO: Doesn't look good in terms of logic composition.
    public LocalVariable registerRowLocalVariable(int nodeId, String suffix, List<LocalVariable> row) {
        String rowVarName = C_UNDERSCORE + nodeId + C_UNDERSCORE
                                + (suffix == null || suffix.isEmpty() ? "" : suffix + C_UNDERSCORE) + "row";

        LocalVariable var = localVariables.get(rowVarName);

        if (var != null) {
            return var;
        }

        var = addLocalVariable(rowVarName, HeapRow.class);

        String[] rowFieldNames = new String[row.size()];

        for (int i = 0; i < rowFieldNames.length; i++) {
            rowFieldNames[i] = row.get(i).getName();
        }

        addContent(var.getClassName(), C_SPACE, var.getName(), C_SPACE, C_EQUALS, C_SPACE)
            .add(invokeStatic(parent, C_HEAP_ROW__M_OF, rowFieldNames)).addContent(C_SEMICOLON).addNewLine();

        return var;
    }

    @Override
    public void emit(Emitter emitter) {
        if (CTOR.equals(name)) {
            emitter.addContent(modifier.getDefinition(), C_SPACE, parent.getSimpleClassName(), C_L_PAR);
        } else {
            emitter.addContent(modifier.getDefinition(), C_SPACE, returnType, C_SPACE, name, C_L_PAR);
        }

        boolean firstArgument = true;

        for (EmitableMethodArgument argument : arguments) {
            if (firstArgument) {
                firstArgument = false;
            } else {
                emitter.addContent(C_COMMA, C_SPACE);
            }

            argument.emit(emitter);
        }

        emitter.addContent(C_R_PAR, C_SPACE, C_L_BRACE);
        emitter.addTab();
        emitter.addNewLine();

        for (EmitterCommand command : commands) {
            emitter.add(command);
        }

        emitter.removeTab();
        emitter.addNewLine();
        emitter.addContent(C_R_BRACE);
    }
}

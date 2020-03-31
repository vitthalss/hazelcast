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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_CLASS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_COMMA;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_DOT;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_EXTENDS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_IMPLEMENTS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_PACKAGE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SEMICOLON;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;

/**
 * Low-level code builder.
 */
@SuppressWarnings("UnusedReturnValue")
public class EmitableClass implements Emitable {
    private final CompilerVisibilityModifier visibilityModifier;
    private final String packageName;
    private final String simpleClassName;
    private final Set<EmitableImport> imports = new TreeSet<>();
    private final LinkedHashSet<String> implementsClasses = new LinkedHashSet<>();
    private final LinkedHashSet<String> extendsClasses = new LinkedHashSet<>();
    private final List<EmitableClassVariable> variables = new ArrayList<>();
    private final List<EmitableMethod> methods = new ArrayList<>();

    public EmitableClass(CompilerVisibilityModifier visibilityModifier, String packageName, String simpleClassName) {
        this.visibilityModifier = visibilityModifier;
        this.packageName = packageName;
        this.simpleClassName = simpleClassName;
    }

    public EmitableClass addImport(Class<?> clazz) {
        return addImport0(clazz, false);
    }

    public EmitableClass addStaticImport(Class<?> clazz) {
        return addImport0(clazz, true);
    }

    private EmitableClass addImport0(Class<?> clazz, boolean isStatic) {
        if (clazz.isPrimitive() || (!isStatic && clazz.getName().startsWith("java.lang"))) {
            // Do not add imports for java.lang staff which is already auto-imported.
            return this;
        }

        imports.add(new EmitableImport(clazz.getPackage().getName(), isStatic));

        return this;
    }

    public EmitableClass addExtends(Class<?> clazz) {
        addImport(clazz);

        if (clazz.isInterface()) {
            implementsClasses.add(CompilerUtils.toSimpleName(clazz));
        } else {
            extendsClasses.add(CompilerUtils.toSimpleName(clazz));
        }

        return this;
    }

    public EmitableClass addVariable(
        CompilerVisibilityModifier visibilityModifier,
        String name,
        Class<?> type,
        Class<?> genericType
    ) {
        addImport(type);

        if (genericType != null) {
            addImport(genericType);
        }

        variables.add(new EmitableClassVariable(visibilityModifier, name, CompilerUtils.toSimpleName(type, genericType)));

        return this;
    }

    public EmitableMethod addConstructor(CompilerVisibilityModifier visibilityModifier) {
        EmitableMethod method = new EmitableMethod(this, visibilityModifier, EmitableMethod.CTOR, null);

        return addMethod0(method);
    }

    public EmitableMethod addMethod(CompilerVisibilityModifier visibilityModifier, String name, Class<?> returnClazz) {
        addImport(returnClazz);

        EmitableMethod method = new EmitableMethod(this, visibilityModifier, name, CompilerUtils.toSimpleName(returnClazz));

        return addMethod0(method);
    }

    private EmitableMethod addMethod0(EmitableMethod method) {
        methods.add(method);

        return method;
    }

    public String getSimpleClassName() {
        return simpleClassName;
    }

    public String getClassName() {
        return packageName + C_DOT + simpleClassName;
    }

    @Override
    public void emit(Emitter emitter) {
        // Package
        emitter.addContent(C_PACKAGE, C_SPACE, packageName, C_SEMICOLON);
        emitter.addNewLine();
        emitter.addNewLine();

        // Imports.
        if (!imports.isEmpty()) {
            for (EmitableImport importStatement : imports) {
                importStatement.emit(emitter);

                emitter.addNewLine();
            }

            emitter.addNewLine();
        }

        // Class definition
        emitter.addContent(visibilityModifier.getDefinition(), C_SPACE, C_CLASS, C_SPACE, simpleClassName, C_SPACE);

        // Extensions.
        emitExtensions(emitter, implementsClasses, true);
        emitExtensions(emitter, extendsClasses, false);

        // Body.
        emitter.addContent(C_SPACE, C_L_BRACE);
        emitter.addTab();
        emitter.addNewLine();

        boolean hasMember = false;

        for (EmitableClassVariable variable : variables) {
            if (hasMember) {
                emitter.addNewLine();
            } else {
                hasMember = true;
            }

            variable.emit(emitter);
        }

        for (EmitableMethod method : methods) {
            if (hasMember) {
                emitter.addNewLine();
                emitter.addNewLine();
            } else {
                hasMember = true;
            }

            method.emit(emitter);
        }

        emitter.removeTab();
        emitter.addNewLine();
        emitter.addContent(C_R_BRACE);
    }

    private void emitExtensions(Emitter emitter, Collection<String> elements, boolean isInterface) {
        if (!elements.isEmpty()) {
            emitter.addContent(isInterface ? C_IMPLEMENTS : C_EXTENDS, C_SPACE);

            boolean first = true;

            for (String element : elements) {
                if (first) {
                    first = false;
                } else {
                    emitter.addContent(C_COMMA, C_SPACE);
                }

                emitter.addContent(element);
            }
        }
    }
}

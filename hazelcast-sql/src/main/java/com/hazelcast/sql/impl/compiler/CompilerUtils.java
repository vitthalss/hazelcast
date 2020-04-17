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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.sql.impl.compiler.emitter.EmitableClass;
import com.hazelcast.sql.impl.compiler.emitter.EmitableMethod;
import com.hazelcast.sql.impl.compiler.emitter.EmitterCommand;
import com.hazelcast.sql.impl.compiler.emitter.EmitterCommands;
import com.hazelcast.sql.impl.compiler.emitter.EmitterContentCommand;
import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.sql.impl.exec.scan.MapScanExecIterator;
import com.hazelcast.sql.impl.exec.scan.MapScanExecUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_COMMA;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_DOT;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_THIS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_VOID;

@SuppressWarnings({"checkstyle:ConstantName", "checkstyle:ExecutableStatementCount"})
public final class CompilerUtils {
    public static final String C_COMPILED_EXEC__M_PREPARE;
    public static final String C_COMPILED_EXEC__M_PREPARE__P_EXEC = "exec";
    public static final String C_COMPILED_EXEC__M_PREPARE__V_EXEC = "exec0";

    public static final String C_COMPILED_EXEC__M_SETUP0;
    public static final String C_COMPILED_EXEC__M_SETUP0__P_CTX = "ctx";

    public static final Method C_COMPILED_EXEC__M_CLEAR_BATCH;
    public static final Method C_COMPILED_EXEC__M_ADD_TO_BATCH;

    public static final Method C_EXEC__M_GET_ID;

    public static final Method C_MAP_SCAN_EXEC__M_GET_MAP;
    public static final Method C_MAP_SCAN_EXEC__M_GET_PARTS;
    public static final Method C_MAP_SCAN_EXEC__M_GET_KEY_DESCRIPTOR;
    public static final Method C_MAP_SCAN_EXEC__M_GET_VALUE_DESCRIPTOR;
    public static final Method C_MAP_SCAN_EXEC__M_GET_FIELD_NAMES;
    public static final Method C_MAP_SCAN_EXEC__M_GET_FIELD_TYPES;
    public static final Method C_MAP_SCAN_EXEC__M_GET_PROJECTS;
    public static final Method C_MAP_SCAN_EXEC__M_GET_FILTER;

    public static final Method C_MAP_SCAN_EXEC_UTILS__M_CREATE_ITERATOR;

    public static final Method C_MAP_SCAN_EXEC_ITERATOR__M_TRY_ADVANCE;
    public static final Method C_MAP_SCAN_EXEC_ITERATOR__M_GET_KEY;
    public static final Method C_MAP_SCAN_EXEC_ITERATOR__M_GET_VALUES;

    public static final Method C_HEAP_ROW__M_OF;

    public static final Method C_EXPRESSION__M_EVAL;

    public static final Field C_ITERATION_RESULT__F_FETCHED;
    public static final Field C_ITERATION_RESULT__F_FETCHED_DONE;

    private CompilerUtils() {
        // No-op.
    }

    static {
        try {
            C_COMPILED_EXEC__M_PREPARE = CompiledExec.class.getDeclaredMethod("prepare", Exec.class).getName();
            C_COMPILED_EXEC__M_SETUP0 = AbstractExec.class.getDeclaredMethod("setup0", QueryFragmentContext.class).getName();
            C_COMPILED_EXEC__M_CLEAR_BATCH = CompiledExec.class.getDeclaredMethod("clearBatch");
            C_COMPILED_EXEC__M_ADD_TO_BATCH = CompiledExec.class.getDeclaredMethod("addToBatch", HeapRow.class);

            C_EXEC__M_GET_ID = Exec.class.getDeclaredMethod("getId");

            C_MAP_SCAN_EXEC__M_GET_MAP = MapScanExec.class.getMethod("getMap");
            C_MAP_SCAN_EXEC__M_GET_PARTS = MapScanExec.class.getMethod("getParts");
            C_MAP_SCAN_EXEC__M_GET_KEY_DESCRIPTOR = MapScanExec.class.getMethod("getKeyDescriptor");
            C_MAP_SCAN_EXEC__M_GET_VALUE_DESCRIPTOR = MapScanExec.class.getMethod("getValueDescriptor");
            C_MAP_SCAN_EXEC__M_GET_FIELD_NAMES = MapScanExec.class.getMethod("getFieldNames");
            C_MAP_SCAN_EXEC__M_GET_FIELD_TYPES = MapScanExec.class.getMethod("getFieldTypes");
            C_MAP_SCAN_EXEC__M_GET_PROJECTS = MapScanExec.class.getMethod("getProjects");
            C_MAP_SCAN_EXEC__M_GET_FILTER = MapScanExec.class.getMethod("getFilter");

            C_MAP_SCAN_EXEC_UTILS__M_CREATE_ITERATOR = MapScanExecUtils.class.getMethod(
                "createIterator", MapContainer.class, PartitionIdSet.class
            );

            C_MAP_SCAN_EXEC_ITERATOR__M_TRY_ADVANCE = MapScanExecIterator.class.getMethod("tryAdvance");
            C_MAP_SCAN_EXEC_ITERATOR__M_GET_KEY = MapScanExecIterator.class.getMethod("getKey");
            C_MAP_SCAN_EXEC_ITERATOR__M_GET_VALUES = MapScanExecIterator.class.getMethod("getValue");

            C_HEAP_ROW__M_OF = HeapRow.class.getMethod("of", Object[].class);

            C_EXPRESSION__M_EVAL = Expression.class.getMethod("eval", Row.class, ExpressionEvalContext.class);

            C_ITERATION_RESULT__F_FETCHED = IterationResult.class.getField("FETCHED");
            C_ITERATION_RESULT__F_FETCHED_DONE = IterationResult.class.getField("FETCHED_DONE");
        } catch (ReflectiveOperationException e) {
            // TODO: Proper handling.
            throw new HazelcastException("Failed to initialize CompilerUtils.", e);
        }
    }

    public static String toSimpleName(Class<?> clazz) {
        return toSimpleName(clazz, null);
    }

    public static String toSimpleName(Class<?> clazz, Class<?> genericClazz) {
        if (clazz == Void.class) {
            return C_VOID;
        }

        if (genericClazz == null) {
            return clazz.getSimpleName();
        } else {
            return clazz.getSimpleName() + "<" + genericClazz.getSimpleName() + ">";
        }
    }

    public static String thisVar(String name) {
        return C_THIS + C_DOT + name;
    }

    public static EmitterCommand invokeStatic(EmitableClass owningClass, Method method, String... params) {
        owningClass.addImport(method.getDeclaringClass());

        return invoke(method.getDeclaringClass().getSimpleName(), method, params);
    }

    public static EmitterCommand invoke(String target, Method method, String... params) {
        return invoke(target, method.getName(), params);
    }

    public static EmitterCommand invoke(String target, String methodName, String... params) {
        EmitterContentCommand res = EmitterCommands.content(
            target,
            C_DOT,
            methodName,
            C_L_PAR
        );

        appendParams(res, params);

        res.append(C_R_PAR);

        return res;
    }

    public static EmitterCommand fieldStatic(EmitableMethod targetMethod, Field field) {
        Class<?> fieldDeclaringClass = field.getDeclaringClass();

        targetMethod.getParent().addImport(field.getDeclaringClass());

        return EmitterCommands.content(fieldDeclaringClass.getSimpleName(), C_DOT, field.getName());
    }

    private static void appendParams(EmitterContentCommand command, String[] params) {
        if (params != null) {
            boolean first = true;

            for (String param : params) {
                if (first) {
                    first = false;
                } else {
                    command.append(C_COMMA, C_SPACE);
                }

                command.append(param);
            }
        }
    }
}

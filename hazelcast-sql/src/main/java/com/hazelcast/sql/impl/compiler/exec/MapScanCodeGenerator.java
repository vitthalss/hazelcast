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

import com.hazelcast.query.QueryConstants;
import com.hazelcast.sql.impl.compiler.CompilerExecVariableType;
import com.hazelcast.sql.impl.compiler.ExecVariable;
import com.hazelcast.sql.impl.compiler.LocalVariable;
import com.hazelcast.sql.impl.compiler.SqlCompiler;
import com.hazelcast.sql.impl.compiler.emitter.EmitableMethod;
import com.hazelcast.sql.impl.compiler.expression.ExpressionCompiler;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.JavaClassQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_COMPILED_EXEC__M_PREPARE__V_EXEC;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC_ITERATOR__M_GET_KEY;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC_ITERATOR__M_GET_VALUES;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC_ITERATOR__M_TRY_ADVANCE;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC_UTILS__M_CREATE_ITERATOR;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC__M_GET_FIELD_PATHS;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC__M_GET_FIELD_TYPES;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC__M_GET_FILTER;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC__M_GET_KEY_DESCRIPTOR;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC__M_GET_MAP;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC__M_GET_PARTITIONS;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC__M_GET_PROJECTS;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_MAP_SCAN_EXEC__M_GET_VALUE_DESCRIPTOR;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.invoke;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.invokeStatic;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.thisVar;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_EQUALS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SEMICOLON;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_UNDERSCORE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_WHILE;

public class MapScanCodeGenerator extends CodeGenerator<MapScanPlanNode> {
    private ExecVariable varFilter;
    private ExecVariable varRecordIterator;

    private LocalVariable keyVar;
    private LocalVariable valueVar;

    public MapScanCodeGenerator(MapScanPlanNode node) {
        super(node);
    }

    @Override
    public void prepare(SqlCompiler compiler) {
        // MapProxyImpl map = exec.getMap();
        ExecVariable varMap = addExecVariableWithPrepare(
            compiler,
            CompilerExecVariableType.MAP_SCAN_MAP,
            (clazz, method) -> method.add(invoke(C_COMPILED_EXEC__M_PREPARE__V_EXEC, C_MAP_SCAN_EXEC__M_GET_MAP))
        );

        // PartitionIdSet parts = exec.getParts();
        ExecVariable varParts = addExecVariableWithPrepare(
            compiler,
            CompilerExecVariableType.MAP_SCAN_PARTS,
            (clazz, method) -> method.add(invoke(C_COMPILED_EXEC__M_PREPARE__V_EXEC, C_MAP_SCAN_EXEC__M_GET_PARTITIONS))
        );

        // Type metadata.
        addExecVariableWithPrepare(
            compiler,
            CompilerExecVariableType.MAP_SCAN_KEY_DESCRIPTOR,
            (clazz, method) -> method.add(invoke(C_COMPILED_EXEC__M_PREPARE__V_EXEC, C_MAP_SCAN_EXEC__M_GET_KEY_DESCRIPTOR))
        );

        addExecVariableWithPrepare(
            compiler,
            CompilerExecVariableType.MAP_SCAN_VALUE_DESCRIPTOR,
            (clazz, method) -> method.add(invoke(C_COMPILED_EXEC__M_PREPARE__V_EXEC, C_MAP_SCAN_EXEC__M_GET_VALUE_DESCRIPTOR))
        );

        addExecVariableWithPrepare(
            compiler,
            CompilerExecVariableType.MAP_SCAN_FIELD_PATHS,
            (clazz, method) -> method.add(invoke(C_COMPILED_EXEC__M_PREPARE__V_EXEC, C_MAP_SCAN_EXEC__M_GET_FIELD_PATHS))
        );

        addExecVariableWithPrepare(
            compiler,
            CompilerExecVariableType.MAP_SCAN_FIELD_TYPES,
            (clazz, method) -> method.add(invoke(C_COMPILED_EXEC__M_PREPARE__V_EXEC, C_MAP_SCAN_EXEC__M_GET_FIELD_TYPES))
        );

        // Project and filter.
        addExecVariableWithPrepare(
            compiler,
            CompilerExecVariableType.MAP_SCAN_PROJECTS,
            (clazz, method) -> method.add(invoke(C_COMPILED_EXEC__M_PREPARE__V_EXEC, C_MAP_SCAN_EXEC__M_GET_PROJECTS))
        );

        varFilter = addExecVariableWithPrepare(
            compiler,
            CompilerExecVariableType.MAP_SCAN_FILTER,
            (clazz, method) -> method.add(invoke(C_COMPILED_EXEC__M_PREPARE__V_EXEC, C_MAP_SCAN_EXEC__M_GET_FILTER))
        );

        // Record iterator.
        varRecordIterator = addExecVariableWithSetup(
            compiler,
            CompilerExecVariableType.MAP_SCAN_RECORD_ITERATOR,
            (clazz, method) -> method.add(
                invokeStatic(clazz, C_MAP_SCAN_EXEC_UTILS__M_CREATE_ITERATOR, varMap.getName(), varParts.getName())
            )
        );
    }

    @Override
    public List<LocalVariable> advanceStart(EmitableMethod method, List<List<LocalVariable>> inputs) {
        // while (recordIterator.tryAdvance()) {
        method.addContent(C_WHILE, C_SPACE, C_L_PAR)
            .add(invoke(thisVar(varRecordIterator.getName()), C_MAP_SCAN_EXEC_ITERATOR__M_TRY_ADVANCE))
            .addContent(C_R_PAR, C_SPACE, C_L_BRACE)
            .addTab()
            .addNewLine();

        // Get all fields which constitutes the row.
        List<LocalVariable> extractedRow = extractFields(method);

        // TODO: Apply the filter (actually we need to apply it during field extraction, to avoid extraction of unnecessary
        //  fields if the filter is not satisfied.

        // Apply the filter.
        applyFilter(method, extractedRow, node.getFilter());

        // Apply the projection.
        List<Integer> projects = node.getProjects();

        List<LocalVariable> projectedRow = new ArrayList<>(projects.size());

        for (Integer project : projects) {
            projectedRow.add(extractedRow.get(project));
        }

        return projectedRow;
    }

    @Override
    public void advanceFinish(EmitableMethod method) {
        method.removeTab().addNewLine().addContent(C_R_BRACE);
    }

    private void applyFilter(EmitableMethod method, List<LocalVariable> row, Expression<Boolean> filter) {
        if (filter == null) {
            return;
        }

        // TODO: In prod code we must apply the filter in the middle of field processing to avoid getting unnecessary fields if
        //  filter check fails.

        // Prepare filter variable.
        LocalVariable filterVar = ExpressionCompiler.INSTANCE.compile(
            method,
            row,
            filter,
            varFilter.getName(),
            node.getId(),
            "filter"
        );

        // Evaluate.
        // TODO: Remove magic constants.
        method.addContent("if (", filterVar.getName(), " == null || !((Boolean) ", filterVar.getName(), ")) {")
            .addTab().addNewLine();

        method.addContent("continue;").removeTab().addNewLine();
        method.addContent("}").addNewLine();
    }

    @Override
    public Class<? extends Exec> getExecClass() {
        return MapScanExec.class;
    }

    @Override
    public boolean isTerminal() {
        return false;
    }

    private List<LocalVariable> extractFields(EmitableMethod method) {
        // Initialize fields.
        List<QueryPath> fieldPaths = node.getFieldPaths();
        List<QueryDataType> fieldTypes = node.getFieldTypes();

        List<LocalVariable> fields = new ArrayList<>(fieldPaths.size());

        for (int i = 0; i < fieldPaths.size(); i++) {
            QueryPath fieldPath = fieldPaths.get(i);
            QueryDataType fieldType = fieldTypes.get(i);

            LocalVariable field = extractJavaField(method, fieldPath, fieldType);

            fields.add(field);
        }

        return fields;
    }

    private LocalVariable extractJavaField(EmitableMethod method, QueryPath fieldPath, QueryDataType fieldType) {
        // TODO: Custom extractors are not supported at the moment. We need Extractor's instance for this.
        if (fieldPath.isTop()) {
            return extractJavaTopObject(method, fieldPath.isKey());
        }

        // Get parent object.
        LocalVariable targetVar = extractJavaTopObject(method, fieldPath.isKey());

        // Register local variable.
        String varName = getScanFieldVariableName(fieldPath.getPath());
        Class<?> varClass = fieldType.getConverter().getValueClass();

        LocalVariable fieldVar = method.addLocalVariable(varName, varClass);

        // Emit the code
        String methodName = getGetterName(fieldPath.getPath(), fieldType);

        method.addContent(fieldVar.getClassName(), C_SPACE, fieldVar.getName(), C_SPACE, C_EQUALS, C_SPACE)
            .add(invoke(targetVar.getName(), methodName)).addContent(C_SEMICOLON).addNewLine();

        return fieldVar;
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    private LocalVariable extractJavaTopObject(EmitableMethod method, boolean isKey) {
        // Return early if already initialized.
        if (isKey && keyVar != null) {
            return keyVar;
        }

        if (!isKey && valueVar != null) {
            return valueVar;
        }

        // Register the variable in the compiler.
        String name = isKey ? QueryConstants.KEY_ATTRIBUTE_NAME.value() : QueryConstants.THIS_ATTRIBUTE_NAME.value();

        QueryTargetDescriptor descriptor = isKey ? node.getKeyDescriptor() : node.getValueDescriptor();

        if (!(descriptor instanceof JavaClassQueryTargetDescriptor)) {
            throw new UnsupportedOperationException("Only JavaClassQueryTargetDescriptor is supported.");
        }

        String className = ((JavaClassQueryTargetDescriptor) descriptor).getClassName();

        LocalVariable res = method.addLocalVariable(getScanFieldVariableName(name), className);

        // Emit the code.
        Method iteratorMethod = isKey ? C_MAP_SCAN_EXEC_ITERATOR__M_GET_KEY : C_MAP_SCAN_EXEC_ITERATOR__M_GET_VALUES;

        method.addContent(
            res.getClassName(), C_SPACE, res.getName(), C_SPACE, C_EQUALS, C_SPACE, C_L_PAR, res.getClassName(), C_R_PAR, C_SPACE
        ).add(invoke(varRecordIterator.getName(), iteratorMethod)).addContent(C_SEMICOLON).addNewLine();

        // Cache the variable to avoid double emission.
        if (isKey) {
            keyVar = res;
        } else {
            valueVar = res;
        }

        return res;
    }

    private String getScanFieldVariableName(String path) {
        return C_UNDERSCORE + getNodeId() + C_UNDERSCORE + path.replace(".", "_");
    }

    private static String getGetterName(String fieldPath, QueryDataType fieldType) {
        String fieldName;

        int dotIndex = fieldPath.indexOf(".");

        if (dotIndex != -1) {
             fieldName = fieldPath.substring(dotIndex + 1);

            // TODO: Nested fields are not supported at the moment.
             assert !fieldName.contains(".");
        } else {
            fieldName = fieldPath;
        }

        // TODO: We need nullability flag here, to distinguish between "is" and "get" prefixes. This is why we pass field type.
        return "get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
    }
}

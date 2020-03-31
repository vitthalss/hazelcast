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
import com.hazelcast.sql.impl.compiler.emitter.EmitableClass;
import com.hazelcast.sql.impl.compiler.emitter.EmitableMethod;
import com.hazelcast.sql.impl.compiler.emitter.Emitter;
import com.hazelcast.sql.impl.compiler.emitter.EmitterCommands;
import com.hazelcast.sql.impl.compiler.emitter.EmitterContentCommand;
import com.hazelcast.sql.impl.compiler.exec.AdvanceCodeGeneratorVisitor;
import com.hazelcast.sql.impl.compiler.exec.CodeGenerator;
import com.hazelcast.sql.impl.compiler.exec.PrepareCodeGeneratorVisitor;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.fragment.QueryFragmentContext;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_COMPILED_EXEC__M_CLEAR_BATCH;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_COMPILED_EXEC__M_PREPARE__V_EXEC;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_COMPILED_EXEC__M_PREPARE__P_EXEC;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_COMPILED_EXEC__M_SETUP0__P_CTX;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_EXEC__M_GET_ID;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.C_ITERATION_RESULT__F_FETCHED_DONE;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.fieldStatic;
import static com.hazelcast.sql.impl.compiler.CompilerUtils.invoke;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_COMMA;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_EQUALS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_EQUALS_EQUALS;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_IF;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_INT_ARRAY;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_L_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_NEW;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_RETURN;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_BRACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_R_PAR;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SEMICOLON;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SUPER;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_THIS;

public class SqlCompiler {
    /** Index generator for class names. */
    private static final AtomicLong NAME_INDEX_GENERATOR = new AtomicLong();

    /** Root node for code generation. */
    private final CodeGenerator<?> root;

    /** Map from node ID to it's variables. */
    private final Map<Integer, List<ExecVariable>> nodeIdToExecVariables = new LinkedHashMap<>();

    public SqlCompiler(CodeGenerator<?> root) {
        this.root = root;
    }

    public CompilerResult compile() {
        // TODO: Magic constant
        String packageName = "com.hazelcast.sql.impl.compiler.compiled";

        // TODO: We'd better to use plan fingerprint here to facilitate debugging
        String simpleClassName = "CompiledExec_" + NAME_INDEX_GENERATOR.incrementAndGet();

        // Prepare class.
        EmitableClass clazz = new EmitableClass(
            CompilerVisibilityModifier.PUBLIC,
            packageName,
            simpleClassName
        );

        clazz.addExtends(CompiledExec.class);

        // Construct the constructor.
        createConstructor(clazz, root.getNode().getId(), root.getAllNodeIds());

        // Construct the state and "prepare" method.
        createPrepareMethod(clazz);

        // Construct "setup" method.
        createSetupMethod(clazz);

        // Construct the body.
        createAdvanceMethod(clazz);

        // Emit the source.
        Emitter emitter = new Emitter();

        clazz.emit(emitter);

        String source = emitter.getContent();

        // Compile.
        Class<? extends CompiledExec> res = compileSource(clazz.getClassName(), source);

        return new CompilerResult(root.getNode(), res, source);
    }

    public void addExecVariable(ExecVariable execVariable) {
        nodeIdToExecVariables.computeIfAbsent(execVariable.getExecId(), (key) -> new ArrayList<>()).add(execVariable);
    }

    private void createConstructor(EmitableClass clazz, int id, Collection<Integer> subsumedIds) {
        EmitableMethod method = clazz.addConstructor(CompilerVisibilityModifier.PUBLIC);

        // new int[] { subsumedIds };
        EmitterContentCommand subsumedIdsContent =
            EmitterCommands.content(C_NEW, C_SPACE, C_INT_ARRAY, C_SPACE, C_L_BRACE, C_SPACE);

        boolean first = true;

        for (int subsumedId : subsumedIds) {
            if (first) {
                first = false;
            } else {
                subsumedIdsContent.append(C_COMMA, C_SPACE);
            }

            subsumedIdsContent.append(subsumedId);
        }

        subsumedIdsContent.append(C_SPACE, C_R_BRACE);

        // super(id, subsumedIds);
        method.addContent(
            C_SUPER, C_L_PAR, id, C_COMMA, C_SPACE, subsumedIdsContent.getContent(), C_R_PAR, C_SEMICOLON
        );
    }

    private void createPrepareMethod(EmitableClass clazz) {
        // TODO: Just pass the method itself and extract all info from it!
        EmitableMethod method = clazz.addMethod(
            CompilerVisibilityModifier.PUBLIC,
            CompilerUtils.C_COMPILED_EXEC__M_PREPARE,
            Void.class
        );

        method.addArgument(C_COMPILED_EXEC__M_PREPARE__P_EXEC, Exec.class);

        try {
            // Let node register their routines.
            root.visit(new PrepareCodeGeneratorVisitor(this));

            // Populate the body.
            for (Map.Entry<Integer, List<ExecVariable>> entry : nodeIdToExecVariables.entrySet()) {
                int nodeId = entry.getKey();


                // if (exec.getId() == NODE_ID) {
                //     ConcreteExecClass exec0 = (ConcreteExecClass) exec;
                //     ...
                // }
                method.addContent(C_IF, C_SPACE, C_L_PAR);
                method.add(CompilerUtils.invoke(C_COMPILED_EXEC__M_PREPARE__P_EXEC, C_EXEC__M_GET_ID));
                method.addContent(C_SPACE, C_EQUALS_EQUALS, C_SPACE, nodeId, C_R_PAR, C_SPACE, C_L_BRACE);
                method.addTab();
                method.addNewLine();

                Class<?> execClass = root.getNodeById(nodeId).getExecClass();

                method.addContent(execClass.getSimpleName(), C_SPACE, C_COMPILED_EXEC__M_PREPARE__V_EXEC, C_SPACE,
                    C_EQUALS, C_SPACE, C_L_PAR, execClass.getSimpleName(), C_R_PAR, C_SPACE,
                    C_COMPILED_EXEC__M_PREPARE__P_EXEC, C_SEMICOLON);

                for (ExecVariable execVariable : entry.getValue()) {
                    clazz.addVariable(
                        CompilerVisibilityModifier.PRIVATE,
                        execVariable.getName(),
                        execVariable.getType().getType(),
                        execVariable.getType().getGenericType()
                    );

                    if (execVariable.hasPrepareCode()) {
                        method.addNewLine();

                        // var = [initialization code];
                        method.addContent(execVariable.getName(), C_SPACE, C_EQUALS, C_SPACE);
                        execVariable.getPrepareCode().generate(clazz, method);
                        method.addContent(C_SEMICOLON);
                    }
                }

                method.removeTab();
                method.addNewLine();
                method.addContent(C_R_BRACE);
            }
        } catch (Exception e) {
            // TODO: Proper handling
            throw new HazelcastException("Failed to prepare compiled exec.", e);
        }
    }

    private void createSetupMethod(EmitableClass clazz) {
        // TODO: Just pass the method itself and extract all info from it!
        EmitableMethod method = clazz.addMethod(
            CompilerVisibilityModifier.PROTECTED,
            CompilerUtils.C_COMPILED_EXEC__M_SETUP0,
            Void.class
        );

        method.addArgument(C_COMPILED_EXEC__M_SETUP0__P_CTX, QueryFragmentContext.class);

        try {
            // Populate the body.
            for (Map.Entry<Integer, List<ExecVariable>> entry : nodeIdToExecVariables.entrySet()) {
                for (ExecVariable execVariable : entry.getValue()) {
                    if (execVariable.hasSetupCode()) {
                        // var = [setup code];
                        method.addContent(execVariable.getName(), C_SPACE, C_EQUALS, C_SPACE);
                        execVariable.getSetupCode().generate(clazz, method);
                        method.addContent(C_SEMICOLON);

                        method.addNewLine();
                    }
                }
            }
        } catch (Exception e) {
            // TODO: Proper handling
            throw new HazelcastException("Failed to prepare compiled exec.", e);
        }
    }

    private void createAdvanceMethod(EmitableClass clazz) {
        EmitableMethod method = clazz.addMethod(CompilerVisibilityModifier.PUBLIC, "advance0", IterationResult.class);

        // Clear the batch.
        method.add(invoke(C_THIS, C_COMPILED_EXEC__M_CLEAR_BATCH)).addContent(C_SEMICOLON).addNewLine();

        // Generate executors code.
        root.visit(new AdvanceCodeGeneratorVisitor(method));

        // Return "done" marker if we haven't returned before, because it is unequivocally means that all data was processed.
        method.addNewLine();
        method.addContent(C_RETURN, C_SPACE).add(fieldStatic(method, C_ITERATION_RESULT__F_FETCHED_DONE)).addContent(C_SEMICOLON);
    }

    /**
     * Compile the source code.
     *
     * @param className Class name.
     * @param source Class source.
     * @return Compiled class.
     */
    @SuppressWarnings("unchecked")
    private Class<? extends CompiledExec> compileSource(String className, String source) {
        SimpleCompiler compiler = new SimpleCompiler();

        try {
            compiler.cook(source);

            ClassLoader classLoader = compiler.getClassLoader();

            return (Class<? extends CompiledExec>) classLoader.loadClass(className);
        } catch (CompileException e) {
            // TODO: Proper handling.
            throw new RuntimeException("Failed to compile.", e);
        } catch (ClassNotFoundException e) {
            // TODO: Proper handling.
            throw new RuntimeException("Failed to get compiled class.", e);
        }
    }
}

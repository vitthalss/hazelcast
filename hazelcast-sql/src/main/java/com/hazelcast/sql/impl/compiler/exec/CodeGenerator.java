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

import com.hazelcast.sql.impl.compiler.CompilerExecVariableType;
import com.hazelcast.sql.impl.compiler.CompilerMethodCodeBlock;
import com.hazelcast.sql.impl.compiler.ExecVariable;
import com.hazelcast.sql.impl.compiler.LocalVariable;
import com.hazelcast.sql.impl.compiler.SqlCompiler;
import com.hazelcast.sql.impl.compiler.emitter.EmitableMethod;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.plan.node.PlanNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class CodeGenerator<T extends PlanNode> {
    /** Current node. */
    protected final T node;

    /** Children nodes. */
    protected final List<CodeGenerator<?>> children;

    /** All nodes, populated lazily.. */
    private volatile Map<Integer, CodeGenerator<?>> allNodes;

    protected CodeGenerator(T node) {
        this(node, null);
    }

    protected CodeGenerator(T node, List<CodeGenerator<?>> children) {
        this.node = node;
        this.children = children != null ? children : Collections.emptyList();
    }

    public PlanNode getNode() {
        return node;
    }

    public int getNodeId() {
        return node.getId();
    }

    public abstract void prepare(SqlCompiler compiler);

    public abstract List<LocalVariable> advanceStart(EmitableMethod method, List<List<LocalVariable>> inputs);

    public abstract void advanceFinish(EmitableMethod method);

    protected ExecVariable addExecVariable(
        SqlCompiler compiler,
        CompilerExecVariableType type
    ) {
        return addExecVariable0(compiler, type, null, null);
    }

    protected ExecVariable addExecVariableWithPrepare(
        SqlCompiler compiler,
        CompilerExecVariableType type,
        CompilerMethodCodeBlock prepareCode
    ) {
        return addExecVariable0(compiler, type, prepareCode, null);
    }

    protected ExecVariable addExecVariableWithSetup(
        SqlCompiler compiler,
        CompilerExecVariableType type,
        CompilerMethodCodeBlock setupCode
    ) {
        return addExecVariable0(compiler, type, null, setupCode);
    }

    protected ExecVariable addExecVariable0(
        SqlCompiler compiler,
        CompilerExecVariableType type,
        CompilerMethodCodeBlock prepareCode,
        CompilerMethodCodeBlock setupCode
    ) {
        ExecVariable variable = new ExecVariable(node.getId(), type, prepareCode, setupCode);

        compiler.addExecVariable(variable);

        return variable;
    }

    public <R> R visit(CodeGeneratorVisitor<R> visitor) {
        if (children != null && !children.isEmpty()) {
            for (CodeGenerator<?> child : children) {
                child.visit(visitor);
            }
        }

        return visitor.visit(this);
    }

    /**
     * @return IDs of all involved physical nodes.
     */
    public Set<Integer> getAllNodeIds() {
        return getAllNodes().keySet();
    }

    public CodeGenerator<?> getNodeById(int id) {
        return getAllNodes().get(id);
    }

    private Map<Integer, CodeGenerator<?>> getAllNodes() {
        if (allNodes == null) {
            NodeMapCodeGeneratorVisitor visitor = new NodeMapCodeGeneratorVisitor();

            this.visit(visitor);

            allNodes = visitor.getNodeMap();
        }

        return allNodes;
    }

    public boolean canCompile() {
        return true;
    }

    public abstract Class<? extends Exec> getExecClass();

    /**
     * @return {@code True} if this is a terminal node, i.e. the node which is not upstream for any other node.
     *   These are root node and sender nodes.
     */
    public abstract boolean isTerminal();
}

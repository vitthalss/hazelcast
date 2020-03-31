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
import com.hazelcast.sql.impl.compiler.emitter.EmitableMethod;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public class AdvanceCodeGeneratorVisitor implements CodeGeneratorVisitor<List<LocalVariable>> {
    /** Target method. */
    private final EmitableMethod method;

    /** Stack of children inputs. */
    private final ArrayDeque<List<LocalVariable>> upstreamInputs = new ArrayDeque<>();

    /** All nodes visited so far in reverse order. */
    private final ArrayDeque<CodeGenerator<?>> visitedNodes = new ArrayDeque<>();

    /** Done flag. */
    private boolean done;

    public AdvanceCodeGeneratorVisitor(EmitableMethod method) {
        this.method = method;
    }

    @Override
    public List<LocalVariable> visit(CodeGenerator<?> node) {
        assert !done : "No nodes are expected after terminal node: " + node;

        // Consume upstream inputs.
        List<List<LocalVariable>> upstreamInputs0 = new ArrayList<>(upstreamInputs);

        upstreamInputs.clear();

        // Visit current node.
        onNodeStart(node);

        List<LocalVariable> input = node.advanceStart(method, upstreamInputs0);

        visitedNodes.addFirst(node);

        // Complete processing of visited nodes if the current node is terminal (including self).
        if (node.isTerminal()) {
            for (CodeGenerator<?> visitedNode : visitedNodes) {
                visitedNode.advanceFinish(method);

                onNodeEnd(visitedNode);
            }

            visitedNodes.clear();
        }

        // Register upstream or mark completion.
        if (input == null) {
            assert node.isTerminal();

            done = true;
        } else {
            upstreamInputs.addLast(input);
        }

        return input;
    }

    private void onNodeStart(CodeGenerator<?> node) {
        method.addNewLine();

        if (node instanceof NonTerminalCodeGenerator) {
            method.addContent("// NODE TERMINAL START: " + node.getNodeId());
        } else {
            method.addContent("// NODE START: " + node.getNodeId() + " (" + node.getNode() + ')');
        }

        method.addNewLine();
    }

    private void onNodeEnd(CodeGenerator<?> node) {
        method.addNewLine();

        if (node instanceof NonTerminalCodeGenerator) {
            method.addContent("// NODE TERMINAL END  : " + node.getNodeId());
        } else {
            method.addContent("// NODE END  : " + node.getNodeId());
        }

        method.addNewLine();
    }
}

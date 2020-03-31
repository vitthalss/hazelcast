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

import com.hazelcast.sql.impl.compiler.exec.CodeGenerator;
import com.hazelcast.sql.impl.compiler.exec.MapScanCodeGenerator;
import com.hazelcast.sql.impl.compiler.exec.NonTerminalCodeGenerator;
import com.hazelcast.sql.impl.compiler.exec.UnsupportedCodeGenerator;
import com.hazelcast.sql.impl.physical.AggregatePhysicalNode;
import com.hazelcast.sql.impl.physical.FetchPhysicalNode;
import com.hazelcast.sql.impl.physical.FilterPhysicalNode;
import com.hazelcast.sql.impl.physical.MapIndexScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.MaterializedInputPhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;
import com.hazelcast.sql.impl.physical.PhysicalNodeWithVisitorCallback;
import com.hazelcast.sql.impl.physical.ProjectPhysicalNode;
import com.hazelcast.sql.impl.physical.ReplicatedMapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.ReplicatedToPartitionedPhysicalNode;
import com.hazelcast.sql.impl.physical.RootPhysicalNode;
import com.hazelcast.sql.impl.physical.SortPhysicalNode;
import com.hazelcast.sql.impl.physical.io.BroadcastSendPhysicalNode;
import com.hazelcast.sql.impl.physical.io.ReceivePhysicalNode;
import com.hazelcast.sql.impl.physical.io.ReceiveSortMergePhysicalNode;
import com.hazelcast.sql.impl.physical.io.UnicastSendPhysicalNode;
import com.hazelcast.sql.impl.physical.join.HashJoinPhysicalNode;
import com.hazelcast.sql.impl.physical.join.NestedLoopJoinPhysicalNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor which prepares code generation fragments.
 */
public class CodeGeneratorPhysicalNodeVisitor implements PhysicalNodeVisitor {
    /** Current tree. */
    private List<CodeGenerator<?>> stack = new ArrayList<>();

    /** Independent compilable fragments. */
    private List<CodeGenerator<?>> fragments = new ArrayList<>();

    /** Whether visitor if finalized. */
    private boolean finalized;

    public CodeGenerator<?> getRoot() {
        if (stack.size() == 1) {
            return pop();
        } else {
            throw new IllegalStateException("Unexpected number of node: " + stack.size());
        }
    }

    public List<CodeGenerator<?>> done() {
        if (!finalized) {
            assert stack.size() == 1;

            addFragmentIfNeeded(pop());

            finalized = true;
        }

        return fragments;
    }

    @Override
    public void onRootNode(RootPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onReceiveNode(ReceivePhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onUnicastSendNode(UnicastSendPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onBroadcastSendNode(BroadcastSendPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onMapScanNode(MapScanPhysicalNode node) {
        push(new MapScanCodeGenerator(node));
    }

    @Override
    public void onMapIndexScanNode(MapIndexScanPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onReplicatedMapScanNode(ReplicatedMapScanPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onSortNode(SortPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onReceiveSortMergeNode(ReceiveSortMergePhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onProjectNode(ProjectPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onFilterNode(FilterPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onAggregateNode(AggregatePhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onNestedLoopJoinNode(NestedLoopJoinPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onHashJoinNode(HashJoinPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onMaterializedInputNode(MaterializedInputPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onReplicatedToPartitionedNode(ReplicatedToPartitionedPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onFetchNode(FetchPhysicalNode node) {
        unsupported(node);
    }

    @Override
    public void onCustomNode(PhysicalNodeWithVisitorCallback node) {
        unsupported(node);
    }

    /**
     * Handle physical node which cannot be compiled. If there are compilable fragments, they are finalized and replaced with
     * the current node. Non-compilable fragments are simply replaced.
     *
     * @param node Node which doesn't support query compilation.
     */
    private void unsupported(PhysicalNode node) {
        // TODO: We should not produce fragments if parent requires reset, but children compiled nodes are not resettable!
        //  At the moment this is only the case for NLJ, which we'd better to remove at all.

        // 1. Consume inputs from the stack.
        int inputCount = node.getInputCount();

        assert inputCount == stack.size();

        for (int i = 0; i < inputCount; i++) {
            addFragmentIfNeeded(pop());
        }

        // 2. Push that node to the stack.
        push(new UnsupportedCodeGenerator(node));
    }

    private void push(CodeGenerator<?> node) {
        stack.add(0, node);
    }

    private CodeGenerator<?> pop() {
        return stack.remove(0);
    }

    private void addFragmentIfNeeded(CodeGenerator<?> node) {
        if (node.canCompile()) {
            CodeGenerator<?> node0 = wrapIntoNonTerminalIfNeeded(node);

            fragments.add(node0);
        }
    }

    /**
     * If the given node is terminal node, return it unchanged. Otherwise, wrap the node into non-terminal node wrapper.
     *
     * @param node Original node.
     * @return Terminal node.
     */
    private CodeGenerator<?> wrapIntoNonTerminalIfNeeded(CodeGenerator<?> node) {
        assert node.canCompile();

        if (node.isTerminal()) {
            return node;
        } else {
            return new NonTerminalCodeGenerator(node);
        }
    }
}

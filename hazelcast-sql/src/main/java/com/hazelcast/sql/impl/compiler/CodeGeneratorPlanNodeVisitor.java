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

import com.hazelcast.sql.impl.plan.node.AggregatePlanNode;
import com.hazelcast.sql.impl.plan.node.FetchPlanNode;
import com.hazelcast.sql.impl.plan.node.FilterPlanNode;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.MaterializedInputPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.ReplicatedMapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.ReplicatedToPartitionedPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.SortPlanNode;
import com.hazelcast.sql.impl.plan.node.io.BroadcastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceiveSortMergePlanNode;
import com.hazelcast.sql.impl.plan.node.io.UnicastSendPlanNode;
import com.hazelcast.sql.impl.plan.node.join.HashJoinPlanNode;
import com.hazelcast.sql.impl.plan.node.join.NestedLoopJoinPlanNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor which prepares code generation fragments.
 */
public class CodeGeneratorPlanNodeVisitor implements PlanNodeVisitor {
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
    public void onRootNode(RootPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onReceiveNode(ReceivePlanNode node) {
        unsupported(node);
    }

    @Override
    public void onUnicastSendNode(UnicastSendPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onBroadcastSendNode(BroadcastSendPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onMapScanNode(MapScanPlanNode node) {
        push(new MapScanCodeGenerator(node));
    }

    @Override
    public void onMapIndexScanNode(MapIndexScanPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onReplicatedMapScanNode(ReplicatedMapScanPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onSortNode(SortPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onReceiveSortMergeNode(ReceiveSortMergePlanNode node) {
        unsupported(node);
    }

    @Override
    public void onProjectNode(ProjectPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onFilterNode(FilterPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onAggregateNode(AggregatePlanNode node) {
        unsupported(node);
    }

    @Override
    public void onNestedLoopJoinNode(NestedLoopJoinPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onHashJoinNode(HashJoinPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onMaterializedInputNode(MaterializedInputPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onReplicatedToPartitionedNode(ReplicatedToPartitionedPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onFetchNode(FetchPlanNode node) {
        unsupported(node);
    }

    @Override
    public void onOtherNode(PlanNode node) {
        unsupported(node);
    }

    /**
     * Handle physical node which cannot be compiled. If there are compilable fragments, they are finalized and replaced with
     * the current node. Non-compilable fragments are simply replaced.
     *
     * @param node Node which doesn't support query compilation.
     */
    private void unsupported(PlanNode node) {
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

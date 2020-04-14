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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.plan.node.PlanNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;

/**
 * Query fragment descriptor which is sent over a wire.
 */
public class QueryExecuteOperationFragment implements IdentifiedDataSerializable {
    private PlanNode node;
    private String signature;
    private QueryExecuteOperationFragmentMapping mapping;
    private Collection<UUID> memberIds;

    public QueryExecuteOperationFragment() {
        // No-op.
    }

    public QueryExecuteOperationFragment(
        PlanNode node,
        String signature,
        QueryExecuteOperationFragmentMapping mapping,
        Collection<UUID> memberIds
    ) {
        this.node = node;
        this.signature = signature;
        this.mapping = mapping;
        this.memberIds = memberIds;
    }

    public PlanNode getNode() {
        return node;
    }

    public String getSignature() {
        return signature;
    }

    public QueryExecuteOperationFragmentMapping getMapping() {
        return mapping;
    }

    public Collection<UUID> getMemberIds() {
        return memberIds;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.OPERATION_EXECUTE_FRAGMENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(node);
        out.writeUTF(signature);
        out.writeInt(mapping.getId());

        if (memberIds != null) {
            out.writeInt(memberIds.size());

            for (UUID mappedMemberId : memberIds) {
                UUIDSerializationUtil.writeUUID(out, mappedMemberId);
            }
        } else {
            out.writeInt(0);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        node = in.readObject();
        signature = in.readUTF();
        mapping = QueryExecuteOperationFragmentMapping.getById(in.readInt());

        int mappedMemberIdsSize = in.readInt();

        if (mappedMemberIdsSize > 0) {
            memberIds = new ArrayList<>(mappedMemberIdsSize);

            for (int i = 0; i < mappedMemberIdsSize; i++) {
                memberIds.add(UUIDSerializationUtil.readUUID(in));
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(signature, node, mapping, memberIds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueryExecuteOperationFragment fragment = (QueryExecuteOperationFragment) o;

        return Objects.equals(signature, fragment.signature) && Objects.equals(node, fragment.node)
                   && Objects.equals(mapping, fragment.mapping) && Objects.equals(memberIds, fragment.memberIds);
    }
}

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

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.sql.impl.exec.scan.MapScanExecIterator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

public enum CompilerExecVariableType {
    MAP_SCAN_MAP(MapContainer.class),
    MAP_SCAN_PARTS(PartitionIdSet.class),
    MAP_SCAN_KEY_DESCRIPTOR(QueryTargetDescriptor.class),
    MAP_SCAN_VALUE_DESCRIPTOR(QueryTargetDescriptor.class),
    MAP_SCAN_FIELD_PATHS(List.class, QueryPath.class),
    MAP_SCAN_FIELD_TYPES(List.class, QueryDataType.class),
    MAP_SCAN_PROJECTS(List.class, Integer.class),
    MAP_SCAN_FILTER(Expression.class, Boolean.class),
    MAP_SCAN_RECORD_ITERATOR(MapScanExecIterator.class);

    private final Class<?> type;
    private final Class<?> genericType;

    CompilerExecVariableType(Class<?> type) {
        this(type, null);
    }

    CompilerExecVariableType(Class<?> type, Class<?> genericType) {
        this.type = type;
        this.genericType = genericType;
    }

    public Class<?> getType() {
        return type;
    }

    public Class<?> getGenericType() {
        return genericType;
    }
}

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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.fragment.QueryFragmentContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.KeyValueRow;
import com.hazelcast.sql.impl.schema.SqlTopObjectDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

/**
 * Common operator for map scans.
 */
public abstract class AbstractMapScanExec extends AbstractExec {
    /** Map name. */
    protected final String mapName;

    /** Key descriptor. */
    protected final SqlTopObjectDescriptor keyDescriptor;

    /** Value descriptor. */
    protected final SqlTopObjectDescriptor valueDescriptor;

    /** Field names. */
    protected final List<String> fieldNames;

    /** Field types. */
    protected final List<QueryDataType> fieldTypes;

    /** Projects. */
    protected final List<Integer> projects;

    /** Filter. */
    protected final Expression<Boolean> filter;

    /** Row to get data with extractors. */
    private KeyValueRow keyValueRow;

    protected AbstractMapScanExec(
        int id,
        String mapName,
        SqlTopObjectDescriptor keyDescriptor,
        SqlTopObjectDescriptor valueDescriptor,
        List<String> fieldNames,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter
    ) {
        super(id);

        this.mapName = mapName;
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.projects = projects;
        this.filter = filter;
    }

    @Override
    protected final void setup0(QueryFragmentContext ctx) {
        keyValueRow = KeyValueRow.create(
            keyDescriptor,
            valueDescriptor,
            fieldNames,
            fieldTypes,
            createExtractors(),
            MapScanExecUtils.getSerializationService(ctx)
        );

        setup1(ctx);
    }

    protected void setup1(QueryFragmentContext ctx) {
        // No-op.
    }

    @Override
    public boolean canReset() {
        return true;
    }

    protected HeapRow prepareRow(Object rawkey, Object rawValue) {
        keyValueRow.setKeyValue(rawkey, rawValue);

        // Filter.
        if (filter != null && !filter.eval(keyValueRow, ctx)) {
            return null;
        }

        // Project.
        HeapRow row = new HeapRow(projects.size());

        for (int j = 0; j < projects.size(); j++) {
            Object projectRes = keyValueRow.get(projects.get(j));

            row.set(j, projectRes);
        }

        return row;
    }

    /**
     * Create extractors for the given operator.
     *
     * @return Extractors for map.
     */
    protected abstract Extractors createExtractors();

    public SqlTopObjectDescriptor getKeyDescriptor() {
        return keyDescriptor;
    }

    public SqlTopObjectDescriptor getValueDescriptor() {
        return valueDescriptor;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<QueryDataType> getFieldTypes() {
        return fieldTypes;
    }

    public List<Integer> getProjects() {
        return projects;
    }

    public Expression<Boolean> getFilter() {
        return filter;
    }
}

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

package com.hazelcast.sql.impl.row;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.schema.SqlTopObjectDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

/**
 * Key-value row. Appears during iteration over a data stored in map or it's index.
 */
public final class KeyValueRow implements Row {
    /** Key descriptor. */
    private final SqlTopObjectDescriptor keyDescriptor;

    /** Value descriptor. */
    private final SqlTopObjectDescriptor valueDescriptor;

    /** Map extractors. */
    private final Extractors extractors;

    /** Field extractors. */
    private final KeyValueFieldExtractor[] fieldExtractors;

    /** Serialization service. */
    private final InternalSerializationService serializationService;

    /** Raw key. */
    private Object rawKey;

    /** Raw value. */
    private Object rawValue;

    /** Key. */
    private Object key;

    /** Value. */
    private Object value;

    private KeyValueRow(
        SqlTopObjectDescriptor keyDescriptor,
        SqlTopObjectDescriptor valueDescriptor,
        Extractors extractors,
        KeyValueFieldExtractor[] fieldExtractors,
        InternalSerializationService serializationService
    ) {
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.extractors = extractors;
        this.fieldExtractors = fieldExtractors;
        this.serializationService = serializationService;
    }

    public static KeyValueRow create(
        SqlTopObjectDescriptor keyDescriptor,
        SqlTopObjectDescriptor valueDescriptor,
        List<String> fieldPaths,
        List<QueryDataType> fieldTypes,
        Extractors extractors,
        InternalSerializationService serializationService
    ) {
        KeyValueFieldExtractor[] fieldExtractors = new KeyValueFieldExtractor[fieldPaths.size()];

        for (int i = 0; i < fieldExtractors.length; i++) {
            fieldExtractors[i] = KeyValueFieldExtractor.create(fieldPaths.get(i), fieldTypes.get(i));
        }

        return new KeyValueRow(keyDescriptor, valueDescriptor, extractors, fieldExtractors, serializationService);
    }

    public void setKeyValue(Object rawKey, Object rawValue) {
        this.rawKey = rawKey;
        this.rawValue = rawValue;

        this.key = null;
        this.value = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int idx) {
        KeyValueFieldExtractor fieldExtractor = fieldExtractors[idx];

        boolean isKey = fieldExtractor.isTargetIsKey();

        return (T) fieldExtractors[idx].get(
            isKey ? getKey() : null,
            isKey ? null : getValue(),
            extractors
        );
    }

    private Object getKey() {
        if (key != null) {
            return key;
        }

        Object res = keyDescriptor.validate(rawKey instanceof Data ? serializationService.toObject(rawKey) : rawKey);

        key = res;

        return res;
    }

    private Object getValue() {
        if (value != null) {
            return value;
        }

        Object res = valueDescriptor.validate(rawValue instanceof Data ? serializationService.toObject(rawValue) : rawValue);

        value = res;

        return res;
    }

    @Override
    public int getColumnCount() {
        return fieldExtractors.length;
    }
}

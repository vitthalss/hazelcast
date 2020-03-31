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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

/**
 * Extract of key/value fields
 */
public final class KeyValueFieldExtractor {
    /** Path which should be used on target. */
    private final String targetPath;

    /** True if key is target, false if value is target. */
    private final boolean targetIsKey;

    /** Whether the field being extracted is top-level object (key or value). */
    private final boolean topObject;

    /** Return type. */
    private final QueryDataType type;

    /** Last observed value class. */
    private transient Class<?> lastClass;

    /** Last observed converter. */
    private transient Converter lastConverter;

    private KeyValueFieldExtractor(String targetPath, boolean targetIsKey, boolean topObject, QueryDataType type) {
        this.targetPath = targetPath;
        this.targetIsKey = targetIsKey;
        this.topObject = topObject;
        this.type = type;
    }

    public static KeyValueFieldExtractor create(String path, QueryDataType type) {
        String targetPath;
        boolean targetIsKey;
        boolean topObject;

        if (KEY_ATTRIBUTE_NAME.value().equals(path)) {
            targetPath = null;
            targetIsKey = true;
            topObject = true;
        } else if (THIS_ATTRIBUTE_NAME.value().equals(path)) {
            targetPath = null;
            targetIsKey = false;
            topObject = true;
        } else {
            topObject = false;

            String keyPath = QueryUtils.extractKeyPath(path);

            if (keyPath != null) {
                targetPath = keyPath;
                targetIsKey = true;
            } else {
                targetPath = path;
                targetIsKey = false;
            }
        }

        return new KeyValueFieldExtractor(targetPath, targetIsKey, topObject, type);
    }

    public Object get(Object key, Object value, Extractors extractors) {
        // Get initial result.
        Object target = targetIsKey ? key : value;
        Object res = topObject ? target : extractors.extract(target, targetPath, null);

        if (res instanceof HazelcastJsonValue) {
            res = Json.parse(res.toString());
        }

        if (res == null) {
            // TODO: Not-null data type must throw an exception here!
            return res;
        }

        // Convert to proper SQL type.
        // TODO: This piece of code may cause severe slowdown. Need to investigate the reason.
        Class<?> resClass = res.getClass();

        if (lastClass != resClass) {
            lastClass = resClass;
            lastConverter = Converters.getConverter(resClass);
        }

        return type.getConverter().convertToSelf(lastConverter, res);
    }

    public boolean isTopObject() {
        return topObject;
    }

    public boolean isTargetIsKey() {
        return targetIsKey;
    }
}

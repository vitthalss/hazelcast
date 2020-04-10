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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.sql.impl.QueryException;

import java.io.IOException;

/**
 * Descriptor of a top-level map object (key or value).
 */
public final class SqlTopObjectDescriptor {
    private final String javaClassName;
    private final int portableFactoryId;
    private final int portableClassId;

    private transient volatile Class<?> javaClass;

    private SqlTopObjectDescriptor(String javaClassName, int portableFactoryId, int portableClassId) {
        this.javaClassName = javaClassName;
        this.portableFactoryId = portableFactoryId;
        this.portableClassId = portableClassId;
    }

    public static SqlTopObjectDescriptor forJavaClass(String javaClassName) {
        return new SqlTopObjectDescriptor(javaClassName, 0, 0);
    }

    public static SqlTopObjectDescriptor forPortable(int portableFactoryId, int portableClassId) {
        return new SqlTopObjectDescriptor(null, portableFactoryId, portableClassId);
    }

    public String getJavaClassName() {
        return javaClassName;
    }

    public int getPortableFactoryId() {
        return portableFactoryId;
    }

    public int getPortableClassId() {
        return portableClassId;
    }

    public boolean isJavaClass() {
        return javaClassName != null;
    }

    public boolean isPortable() {
        return javaClassName == null;
    }

    public Object validate(Object target) {
        boolean res;

        if (isJavaClass()) {
            if (javaClass != null) {
                res = target.getClass() == javaClass;
            } else {
                Class<?> targetClass = target.getClass();

                res = target.getClass().getName().equalsIgnoreCase(javaClassName);

                if (res) {
                    javaClass = targetClass;
                }
            }
        } else {
            if (target instanceof Portable) {
                Portable target0 = (Portable) target;

                res = target0.getFactoryId() == portableFactoryId && target0.getClassId() == portableClassId;
            } else {
                res = false;
            }
        }

        if (res) {
            return target;
        } else {
            throw QueryException.error("Unexpected object type: " + target.getClass());
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        if (isJavaClass()) {
            out.writeBoolean(true);
            out.writeUTF(javaClassName);
        } else {
            out.writeBoolean(false);
            out.writeInt(portableFactoryId);
            out.writeInt(portableClassId);
        }
    }

    public static SqlTopObjectDescriptor readData(ObjectDataInput in) throws IOException {
        if (in.readBoolean()) {
            return SqlTopObjectDescriptor.forJavaClass(in.readUTF());
        } else {
            return SqlTopObjectDescriptor.forPortable(in.readInt(), in.readInt());
        }
    }
}

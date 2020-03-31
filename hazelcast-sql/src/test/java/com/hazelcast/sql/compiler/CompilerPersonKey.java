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

package com.hazelcast.sql.compiler;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class CompilerPersonKey implements DataSerializable {
    private String fieldA;
    private String fieldB;

    public CompilerPersonKey() {
        // No-op.
    }

    public CompilerPersonKey(String fieldA, String fieldB) {
        this.fieldA = fieldA;
        this.fieldB = fieldB;
    }

    public String getFieldA() {
        return fieldA;
    }

    public String getFieldB() {
        return fieldB;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(fieldA);
        out.writeUTF(fieldB);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        fieldA = in.readUTF();
        fieldB = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompilerPersonKey that = (CompilerPersonKey) o;

        return Objects.equals(fieldA, that.fieldA) && Objects.equals(fieldB, that.fieldB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldA, fieldB);
    }
}

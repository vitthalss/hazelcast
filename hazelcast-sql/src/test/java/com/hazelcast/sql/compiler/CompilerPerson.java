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

@SuppressWarnings("checkstyle:ParameterName")
public class CompilerPerson implements DataSerializable {
    private int fieldC;
    private int fieldD;
    private String fieldA_val;
    private String fieldB_val;

    public CompilerPerson() {
        // No-op.
    }

    public CompilerPerson(int fieldC, int fieldD, String fieldA_val, String fieldB_val) {
        this.fieldC = fieldC;
        this.fieldD = fieldD;
        this.fieldA_val = fieldA_val;
        this.fieldB_val = fieldB_val;
    }

    public int getFieldC() {
        return fieldC;
    }

    public int getFieldD() {
        return fieldD;
    }

    public String getFieldA_val() {
        return fieldA_val;
    }

    public String getFieldB_val() {
        return fieldB_val;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(fieldC);
        out.writeInt(fieldD);
        out.writeUTF(fieldA_val);
        out.writeUTF(fieldB_val);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        fieldC = in.readInt();
        fieldD = in.readInt();
        fieldA_val = in.readUTF();
        fieldB_val = in.readUTF();
    }
}

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

package com.hazelcast.sql.impl.compiler.emitter;

import java.util.Objects;

import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_IMPORT;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_PACKAGE_WILDCARD;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SEMICOLON;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_STATIC;

public class EmitableImport implements Emitable, Comparable<EmitableImport> {
    private final String packageName;
    private final boolean isStatic;

    public EmitableImport(String packageName, boolean isStatic) {
        this.packageName = packageName;
        this.isStatic = isStatic;
    }

    public String getPackageName() {
        return packageName;
    }

    public boolean isStatic() {
        return isStatic;
    }

    @Override
    public void emit(Emitter emitter) {
        emitter.addContent(C_IMPORT, C_SPACE);

        if (isStatic) {
            emitter.addContent(C_STATIC, C_SPACE);
        }

        emitter.addContent(packageName, C_PACKAGE_WILDCARD, C_SEMICOLON);
    }

    @Override
    public int compareTo(EmitableImport other) {
        int res = packageName.compareTo(other.packageName);

        if (res == 0) {
            res = Boolean.compare(isStatic, other.isStatic);
        }

        return res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EmitableImport anImport = (EmitableImport) o;

        return isStatic == anImport.isStatic && Objects.equals(packageName, anImport.packageName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(packageName, isStatic);
    }
}

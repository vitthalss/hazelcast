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

import com.hazelcast.sql.impl.compiler.CompilerVisibilityModifier;

import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SEMICOLON;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_SPACE;

/**
 * A single variable.
 */
public class EmitableClassVariable implements Emitable {
    private final CompilerVisibilityModifier visibilityModifier;
    private final String name;
    private final String type;

    public EmitableClassVariable(CompilerVisibilityModifier visibilityModifier, String name, String type) {
        this.visibilityModifier = visibilityModifier;
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    @Override
    public void emit(Emitter emitter) {
        emitter.addContent(visibilityModifier.getDefinition(), C_SPACE, type, C_SPACE, name, C_SEMICOLON);
    }
}

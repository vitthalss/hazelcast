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

import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_UNDERSCORE;

/**
 * Executor state variable.
 */
public class ExecVariable {
    private final int execId;
    private final CompilerExecVariableType type;
    private final CompilerMethodCodeBlock prepareCode;
    private final CompilerMethodCodeBlock setupCode;

    public ExecVariable(
        int execId,
        CompilerExecVariableType type,
        CompilerMethodCodeBlock prepareCode,
        CompilerMethodCodeBlock setupCode
    ) {
        this.type = type;
        this.execId = execId;
        this.prepareCode = prepareCode;
        this.setupCode = setupCode;
    }

    public int getExecId() {
        return execId;
    }

    public CompilerExecVariableType getType() {
        return type;
    }

    public String getName() {
        return C_UNDERSCORE + execId + C_UNDERSCORE + getType().name().toLowerCase();
    }

    public boolean hasPrepareCode() {
        return prepareCode != null;
    }

    public CompilerMethodCodeBlock getPrepareCode() {
        return prepareCode;
    }

    public boolean hasSetupCode() {
        return setupCode != null;
    }

    public CompilerMethodCodeBlock getSetupCode() {
        return setupCode;
    }
}

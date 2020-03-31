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

public final class EmitterCommands {
    public static final EmitterCommand NEW_LINE = new EmitterCommand() { };
    public static final EmitterCommand TAB_ADD = new EmitterCommand() { };
    public static final EmitterCommand TAB_REMOVE = new EmitterCommand() { };

    private EmitterCommands() {
        // No-op.
    }

    public static EmitterContentCommand content(Object... data) {
        EmitterContentCommand command = new EmitterContentCommand();

        if (data != null) {
            command.append(data);
        }

        return command;
    }

    public static EmitterCommand newLine() {
        return NEW_LINE;
    }

    public static EmitterCommand addTab() {
        return TAB_ADD;
    }

    public static EmitterCommand removeTab() {
        return TAB_REMOVE;
    }
}

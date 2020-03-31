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

import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_NEW_LINE;
import static com.hazelcast.sql.impl.compiler.emitter.EmitterConstants.C_TAB;

/**
 * Low-level class which collects emitted tokens.
 */
@SuppressWarnings("UnusedReturnValue")
public class Emitter {
    private final StringBuilder content = new StringBuilder();
    private int tabLevel;

    public Emitter add(EmitterCommand command) {
        if (command instanceof EmitterContentCommand) {
            content.append(((EmitterContentCommand) command).getContent());
        } else if (command == EmitterCommands.NEW_LINE) {
            content.append(C_NEW_LINE);

            if (tabLevel > 0) {
                for (int i = 0; i < tabLevel; i++) {
                    content.append(C_TAB);
                }
            }
        } else if (command == EmitterCommands.TAB_ADD) {
            tabLevel++;
        } else if (command == EmitterCommands.TAB_REMOVE) {
            tabLevel--;
        } else {
            throw new IllegalArgumentException("Unknown command: " + command);
        }

        return this;
    }

    public Emitter addContent(Object... data) {
        add(EmitterCommands.content(data));

        return this;
    }

    public Emitter addNewLine() {
        add(EmitterCommands.newLine());

        return this;
    }

    public Emitter addTab() {
        add(EmitterCommands.addTab());

        return this;
    }

    public Emitter removeTab() {
        add(EmitterCommands.removeTab());

        return this;
    }

    public String getContent() {
        return content.toString();
    }
}

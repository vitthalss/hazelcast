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

/**
 * Emitter command which emits a string.
 */
public class EmitterContentCommand implements EmitterCommand {
    private StringBuilder content;

    EmitterContentCommand() {
        // No-op.
    }

    public EmitterContentCommand append(Object... parts) {
        if (content == null) {
            content = new StringBuilder();
        }

        if (parts != null && parts.length > 0) {
            for (Object part : parts) {
                content.append(part);
            }
        }

        return this;
    }

    public String getContent() {
        return content.toString();
    }
}

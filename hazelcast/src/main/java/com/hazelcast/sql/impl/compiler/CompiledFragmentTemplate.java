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

import com.hazelcast.sql.HazelcastSqlException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Compiled fragment template. We use it to create the fragment for the query.
 */
public class CompiledFragmentTemplate {
    /** Compiled classes. */
    private final Collection<Class<? extends CompiledExec>> classes;

    public CompiledFragmentTemplate(Collection<Class<? extends CompiledExec>> classes) {
        this.classes = classes;
    }

    public CompiledFragment newFragment() {
        try {
            Map<Integer, CompiledExec> topExecs = new HashMap<>();
            Map<Integer, CompiledExec> subsumedExecs = new HashMap<>();

            for (Class<? extends CompiledExec> clazz : classes) {
                CompiledExec exec = clazz.newInstance();

                topExecs.put(exec.getId(), exec);

                for (int subsumedId : exec.getSubsumedIds()) {
                    subsumedExecs.put(subsumedId, exec);
                }
            }

            return new CompiledFragment(topExecs, subsumedExecs);
        } catch (ReflectiveOperationException e) {
            // TODO: How to react to this? Normally it should never happen.
            throw HazelcastSqlException.error("Failed to instantiate compiled fragment. ", e);
        }
    }
}

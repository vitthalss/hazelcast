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

public final class EmitterConstants {
    public static final String C_IMPORT = "import";
    public static final String C_STATIC = "static";
    public static final String C_VISIBILITY_PUBLIC = "public";
    public static final String C_VISIBILITY_PROTECTED = "protected";
    public static final String C_VISIBILITY_PRIVATE = "private";
    public static final String C_VISIBILITY_DEFAULT = "";
    public static final String C_PACKAGE = "package";
    public static final String C_PACKAGE_WILDCARD = ".*";
    public static final String C_CLASS = "class";
    public static final String C_SUPER = "super";
    public static final String C_IMPLEMENTS = "implements";
    public static final String C_EXTENDS = "extends";
    public static final String C_VOID = "void";
    public static final String C_NULL = "null";
    public static final String C_RETURN = "return";
    public static final String C_IF = "if";
    public static final String C_NEW = "new";
    public static final String C_WHILE = "while";
    public static final String C_INT_ARRAY = "int[]";
    public static final String C_THIS = "this";

    public static final String C_NEW_LINE = "\n";
    public static final String C_TAB = "    ";

    public static final String C_SPACE = " ";
    public static final String C_DOT = ".";
    public static final String C_COMMA = ",";
    public static final String C_SEMICOLON = ";";
    public static final String C_L_PAR = "(";
    public static final String C_R_PAR = ")";
    public static final String C_L_BRACE = "{";
    public static final String C_R_BRACE = "}";
    public static final String C_UNDERSCORE = "_";
    public static final String C_QUOTE = "\"";
    public static final String C_EQUALS = "=";
    public static final String C_EQUALS_EQUALS = "==";
    public static final String C_PLUS = "+";
    public static final String C_EXCLAMATION = "!";

    private EmitterConstants() {
        // No-op.
    }
}

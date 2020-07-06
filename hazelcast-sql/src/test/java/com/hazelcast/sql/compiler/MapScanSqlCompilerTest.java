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

import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.compiler.CodeGeneratorPlanNodeVisitor;
import com.hazelcast.sql.impl.compiler.CompilerManager;
import com.hazelcast.sql.impl.compiler.CompilerResult;
import com.hazelcast.sql.impl.compiler.SqlCompiler;
import com.hazelcast.sql.impl.compiler.exec.CodeGenerator;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.extract.JavaClassQueryTargetDescriptor;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertNotNull;

public class MapScanSqlCompilerTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        CompilerManager.TEST_ENABLED = true;
    }

    @AfterClass
    public static void afterClass() {
        CompilerManager.TEST_ENABLED = false;
    }

    @Test
    public void testMapScanCompilation() {
        MapScanPlanNode mapScanNode = new MapScanPlanNode(
            1,
            "MyMap",
            new JavaClassQueryTargetDescriptor(CompilerPersonKey.class.getName()),
            new JavaClassQueryTargetDescriptor(CompilerPerson.class.getName()),
            Arrays.asList(valuePath("fieldC"), valuePath("fieldD")),
            Arrays.asList(QueryDataType.INT, QueryDataType.INT),
            Arrays.asList(0, 1),
            ComparisonPredicate.create(
                PlusFunction.create(
                    ColumnExpression.create(0, QueryDataType.INT),
                    ColumnExpression.create(1, QueryDataType.INT),
                    QueryDataType.INT
                ),
                ColumnExpression.create(1, QueryDataType.INT),
                ComparisonMode.EQUALS
            )
        );

        RootPlanNode rootNode = new RootPlanNode(2, mapScanNode);

        CodeGeneratorPlanNodeVisitor visitor = new CodeGeneratorPlanNodeVisitor();

        rootNode.visit(visitor);

        CodeGenerator<?> codeGenerator = visitor.done().get(0);

        SqlCompiler compiler = new SqlCompiler(codeGenerator);

        CompilerResult res = compiler.compile();

        String source = res.getSource();

        System.out.println(">>> SOURCE:");
        System.out.println(source);

        assertNotNull(source);
    }
}

/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import junit.framework.TestCase;

import java.math.BigInteger;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;

public class SqlStatementLongOverflowTest extends TestCase {

    public void testGetLongObjectWithinRange() throws Exception {
        final ResultSet resultSet = mockResultSet(
            BigInteger.valueOf(Long.MAX_VALUE));
        assertEquals(
            Long.valueOf(Long.MAX_VALUE),
            SqlStatement.getLongObject(resultSet, 1));
    }

    public void testGetLongObjectOverflowThrows() throws Exception {
        final ResultSet resultSet = mockResultSet(
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        try {
            SqlStatement.getLongObject(resultSet, 1);
            fail("Expected SQLDataException");
        } catch (SQLDataException e) {
            assertTrue(e.getMessage().contains("cannot be represented as Java long"));
            assertTrue(e.getMessage().contains("UInt64"));
        }
    }

    public void testGetLongObjectNull() throws Exception {
        final ResultSet resultSet = mockResultSet(null);
        assertNull(SqlStatement.getLongObject(resultSet, 1));
    }

    private ResultSet mockResultSet(final Object value) {
        final ResultSetMetaData metaData = (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class[] { ResultSetMetaData.class },
            new InvocationHandler() {
                public Object invoke(Object proxy, Method method, Object[] args) {
                    final String name = method.getName();
                    if ("getColumnName".equals(name)) {
                        return "test_col";
                    }
                    if ("getColumnTypeName".equals(name)) {
                        return "UInt64";
                    }
                    if ("unwrap".equals(name)) {
                        return null;
                    }
                    if ("isWrapperFor".equals(name)) {
                        return false;
                    }
                    return defaultValue(method.getReturnType());
                }
            });

        return (ResultSet) Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(),
            new Class[] { ResultSet.class },
            new InvocationHandler() {
                public Object invoke(Object proxy, Method method, Object[] args) {
                    final String name = method.getName();
                    if ("getObject".equals(name)) {
                        return value;
                    }
                    if ("getMetaData".equals(name)) {
                        return metaData;
                    }
                    if ("wasNull".equals(name)) {
                        return value == null;
                    }
                    if ("unwrap".equals(name)) {
                        return null;
                    }
                    if ("isWrapperFor".equals(name)) {
                        return false;
                    }
                    return defaultValue(method.getReturnType());
                }
            });
    }

    private Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (returnType == Boolean.TYPE) {
            return false;
        }
        if (returnType == Byte.TYPE) {
            return (byte) 0;
        }
        if (returnType == Short.TYPE) {
            return (short) 0;
        }
        if (returnType == Integer.TYPE) {
            return 0;
        }
        if (returnType == Long.TYPE) {
            return 0L;
        }
        if (returnType == Float.TYPE) {
            return 0f;
        }
        if (returnType == Double.TYPE) {
            return 0d;
        }
        if (returnType == Character.TYPE) {
            return '\0';
        }
        return null;
    }
}

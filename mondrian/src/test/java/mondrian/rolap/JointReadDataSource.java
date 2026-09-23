package mondrian.rolap;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Predicate;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;

/** Real H2 rows with a deterministic boundary for interruption/cache races. */
final class JointReadDataSource {
    interface RowListener {
        void afterNext(int row, boolean more) throws SQLException;
    }

    static DataSource create(String jdbc, Predicate<String> selected, RowListener listener) {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL(jdbc);
        source.setUser("sa");
        source.setPassword("");
        return proxy(DataSource.class, (ignored, method, args) -> {
            Object value = invoke(source, method, args);
            if (!(value instanceof Connection connection)) {
                return value;
            }
            return proxy(Connection.class, (ignoredConnection, connectionMethod, connectionArgs) -> {
                Object statementValue = invoke(connection, connectionMethod, connectionArgs);
                if (!(statementValue instanceof Statement statement)) {
                    return statementValue;
                }
                return proxy(Statement.class, (ignoredStatement, statementMethod, statementArgs) -> {
                    Object resultValue = invoke(statement, statementMethod, statementArgs);
                    if (!(resultValue instanceof ResultSet resultSet)
                        || !statementMethod.getName().equals("executeQuery")
                        || !selected.test((String) statementArgs[0]))
                    {
                        return resultValue;
                    }
                    int[] row = {0};
                    return proxy(ResultSet.class, (ignoredResult, resultMethod, resultArgs) -> {
                        Object result = invoke(resultSet, resultMethod, resultArgs);
                        if (resultMethod.getName().equals("next")) {
                            listener.afterNext(++row[0], (Boolean) result);
                        }
                        return result;
                    });
                });
            });
        });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(
            JointReadDataSource.class.getClassLoader(), new Class<?>[] {type}, handler));
    }
}

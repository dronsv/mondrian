/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2006 Robin Bagot and others
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara
// All Rights Reserved.
*/

package mondrian.rolap;

import mondrian.olap.Util;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import javax.sql.DataSource;

/**
 * Singleton class that holds a connection pool.
 * Call RolapConnectionPool.instance().getPoolingDataSource(connectionFactory)
 * to get a DataSource in return that is a pooled data source.
 *
 * Migrated from commons-dbcp 1.x / commons-pool 1.x to commons-dbcp2 /
 * commons-pool2 during the modernization program (BUILD-B3 lane).
 *
 * @author jhyde
 * @author Robin Bagot
 * @since 7 July, 2003
 */
class RolapConnectionPool {

    public static RolapConnectionPool instance() {
        return instance;
    }

    private static final RolapConnectionPool instance =
        new RolapConnectionPool();

    private final Map<Object, ObjectPool<PoolableConnection>> mapConnectKeyToPool =
        new HashMap<>();

    private final Map<Object, DataSource> dataSourceMap =
        new WeakHashMap<>();

    private RolapConnectionPool() {
    }

    /**
     * Sets up a pooling data source for connection pooling.
     */
    public synchronized DataSource getPoolingDataSource(
        Object key,
        ConnectionFactory connectionFactory)
    {
        ObjectPool<PoolableConnection> connectionPool =
            getPool(key, connectionFactory);
        return new PoolingDataSource<>(connectionPool);
    }

    void clearPool() {
        mapConnectKeyToPool.clear();
    }

    public synchronized DataSource getDriverManagerPoolingDataSource(
        String jdbcConnectString,
        Properties jdbcProperties)
    {
        List<Object> key =
            Arrays.<Object>asList(
                "DriverManagerPoolingDataSource",
                jdbcConnectString,
                jdbcProperties);
        DataSource dataSource = dataSourceMap.get(key);
        if (dataSource != null) {
            return dataSource;
        }

        ConnectionFactory connectionFactory =
            new DriverManagerConnectionFactory(
                jdbcConnectString,
                jdbcProperties);

        try {
            String propertyString = jdbcProperties.toString();
            dataSource = getPoolingDataSource(
                jdbcConnectString + propertyString,
                connectionFactory);
        } catch (Throwable e) {
            throw Util.newInternal(
                e,
                "Error while creating connection pool (with URI "
                + jdbcConnectString + ")");
        }
        dataSourceMap.put(key, dataSource);
        return dataSource;
    }

    public synchronized DataSource getDataSourcePoolingDataSource(
        DataSource dataSource,
        String dataSourceName,
        String jdbcUser,
        String jdbcPassword)
    {
        List<Object> key =
            Arrays.asList(
                "DataSourcePoolingDataSource",
                dataSource,
                jdbcUser,
                jdbcPassword);
        DataSource pooledDataSource = dataSourceMap.get(key);
        if (pooledDataSource != null) {
            return pooledDataSource;
        }

        ConnectionFactory connectionFactory;
        if (jdbcUser != null || jdbcPassword != null) {
            connectionFactory =
                new DataSourceConnectionFactory(
                    dataSource, jdbcUser, jdbcPassword);
        } else {
            connectionFactory =
                new DataSourceConnectionFactory(dataSource);
        }
        try {
            pooledDataSource =
                getPoolingDataSource(
                    dataSourceName,
                    connectionFactory);
        } catch (Exception e) {
            throw Util.newInternal(
                e,
                "Error while creating connection pool (with URI "
                + dataSourceName + ")");
        }
        dataSourceMap.put(key, pooledDataSource);
        return dataSource;
    }

    /**
     * Gets or creates a connection pool for a particular connect
     * specification.
     */
    private synchronized ObjectPool<PoolableConnection> getPool(
        Object key,
        ConnectionFactory connectionFactory)
    {
        ObjectPool<PoolableConnection> connectionPool =
            mapConnectKeyToPool.get(key);
        if (connectionPool == null) {
            // Create the PoolableConnectionFactory. In dbcp2 this is a
            // 2-argument constructor instead of the 8-argument dbcp1
            // constructor. Validation query and default settings are set
            // via setters.
            PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);
            poolableConnectionFactory.setDefaultReadOnly(false);
            poolableConnectionFactory.setDefaultAutoCommit(true);
            // Validation query left null — same as original dbcp1 config.

            // Configure the generic object pool. dbcp2/pool2 uses a config
            // object instead of the 10-argument constructor from pool1.
            GenericObjectPoolConfig<PoolableConnection> poolConfig =
                new GenericObjectPoolConfig<>();
            poolConfig.setMaxTotal(50); // was maxActive in pool1
            poolConfig.setMaxIdle(10);
            poolConfig.setMinIdle(0);
            poolConfig.setMaxWait(Duration.ofMillis(3000));
            poolConfig.setTestOnBorrow(false);
            poolConfig.setTestOnReturn(false);
            poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(60000));
            poolConfig.setNumTestsPerEvictionRun(5);
            poolConfig.setMinEvictableIdleDuration(Duration.ofMillis(30000));
            poolConfig.setTestWhileIdle(true);
            poolConfig.setBlockWhenExhausted(true); // was WHEN_EXHAUSTED_BLOCK

            // Abandoned connection config.
            AbandonedConfig abandonedConfig = new AbandonedConfig();
            abandonedConfig.setRemoveAbandonedOnBorrow(true);
            abandonedConfig.setRemoveAbandonedTimeout(Duration.ofSeconds(300));
            abandonedConfig.setLogAbandoned(true);

            // Create the pool. pool2 uses a 3-arg constructor:
            // (factory, config, abandonedConfig).
            GenericObjectPool<PoolableConnection> pool =
                new GenericObjectPool<>(
                    poolableConnectionFactory,
                    poolConfig,
                    abandonedConfig);

            // Wire the factory back to the pool (dbcp2 requirement).
            poolableConnectionFactory.setPool(pool);

            connectionPool = pool;
            mapConnectKeyToPool.put(key, connectionPool);
        }
        return connectionPool;
    }

}

// End RolapConnectionPool.java

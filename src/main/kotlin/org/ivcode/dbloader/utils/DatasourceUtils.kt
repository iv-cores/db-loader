package org.ivcode.dbloader.utils

import org.apache.commons.dbcp.BasicDataSource
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import javax.sql.DataSource

fun dataSource(init: BasicDataSource.() -> Unit): DataSource {
    val dataSource = BasicDataSource()
    dataSource.init()

    return dataSource
}

fun getDriverClassName(connection: Connection): String {
    return DriverManager.getDriver(connection.metaData.url).javaClass.name
}

fun getDriverClassName(connection: org.sql2o.Connection): String {
    return getDriverClassName(connection.jdbcConnection)
}

fun ResultSet.forEachRow(action: ResultSet.() -> Unit) {
    while (next()) {
        action()
    }
}

fun ResultSet.useEach(action: ResultSet.() -> Unit) {
    use {
        forEachRow(action)
    }
}

fun org.sql2o.Connection.useReadOnly(action: (org.sql2o.Connection) -> Unit) = use {
    val readOnly = it.jdbcConnection.isReadOnly
    it.jdbcConnection.isReadOnly = true
    try {
        action(it)
    } finally {
        it.jdbcConnection.isReadOnly = readOnly
    }
}


fun org.sql2o.Connection.useTransaction(dryRun: Boolean = false, action: (org.sql2o.Connection) -> Unit) = use {
    val autoCommit = it.jdbcConnection.autoCommit
    it.jdbcConnection.autoCommit = false
    try {
        action(it)
        if(!dryRun) {
            it.jdbcConnection.commit()
        } else {
            LoggerFactory.getLogger("org.ivcode.dbloader.utils.useTransaction").info("Dry Run: Rolling back transaction")
            it.jdbcConnection.rollback()
        }
    } catch (e: Exception) {
        it.jdbcConnection.rollback()
        throw e
    } finally {
        it.jdbcConnection.autoCommit = autoCommit
    }
}

package org.ivcode.dbloader.metadata

import org.ivcode.dbloader.utils.useEach
import org.sql2o.Connection
import java.sql.DatabaseMetaData
import java.sql.JDBCType

private val IGNORE_CATALOGS = mapOf(
    "mysql" to setOf("information_schema", "performance_schema", "sys"),
    "postgres" to setOf("postgres", "template0", "template1")
)

data class DatabaseMetaData (
    val catalog: String,
    val tables: List<TableMetaData>
)

data class TableMetaData (
    val name: String,
    val columns: List<ColumnMetaData>
)

data class ColumnMetaData (
    val name: String,
    val type: JDBCType,
    val typeName: String,
    val nullable: Boolean,
    val columnDef: String?,
    val autoincrement: Boolean?,
)

data class SchemaMetaData (
    val catalog: String,
    val schema: String
)

fun createDatabaseMetaData(
    dialect: String,
    catalog: String? = null,
    conn: Connection
): org.ivcode.dbloader.metadata.DatabaseMetaData {
    return createDatabaseMetaData(dialect, catalog, conn.jdbcConnection.metaData)
}

private fun createDatabaseMetaData(
    dialect: String,
    catalog: String?,
    metadata: DatabaseMetaData
): org.ivcode.dbloader.metadata.DatabaseMetaData {
    val catalog = getCatalog(dialect, catalog, metadata) ?: throw IllegalArgumentException("No catalog found")
    return DatabaseMetaData(
        catalog = catalog,
        tables = createTableMetaData(metadata, catalog)
    )
}

private fun getCatalog(dialect: String, catalog: String?, metadata: DatabaseMetaData): String? {
    val catalogs = getCatalogs(dialect, catalog, metadata)

    return when {
        catalogs.size > 1 -> throw IllegalArgumentException("Multiple catalogs found")
        catalogs.isEmpty() -> null
        else -> catalogs.first()
    }
}

private fun getCatalogs(dialect: String, catalog: String?, metadata: DatabaseMetaData): List<String> {
    val ignoreSet = IGNORE_CATALOGS[dialect] ?: emptyList()

    val catalogs = mutableListOf<String>()
    metadata.catalogs.useEach {
        val cat = getString("TABLE_CAT")
        if ((catalog != null && cat == catalog) || (catalog == null && cat !in ignoreSet)) {
            catalogs.add(cat)
        }
    }

    return catalogs
}

private fun getSchema(metadata: DatabaseMetaData): SchemaMetaData {
    val schemas = getSchemas(metadata)

    return when {
        schemas.size > 1 -> throw IllegalArgumentException("Multiple schemas found")
        schemas.isEmpty() -> throw IllegalArgumentException("No schemas found")
        else -> schemas.first()
    }
}

private fun getSchemas(metadata: DatabaseMetaData): List<SchemaMetaData> {
    val schemas = mutableListOf<SchemaMetaData>()

    metadata.schemas.useEach {
        schemas.add(SchemaMetaData(
            catalog = getString("TABLE_CATALOG"),
            schema = getString("TABLE_SCHEM")
        ))
    }

    return schemas
}

private fun createTableMetaData(metadata: DatabaseMetaData, catalog: String): List<TableMetaData> {
    // providers are used to defer pulling column information until we've closed the current table result set

    val tableProviders = mutableListOf<()->TableMetaData>()
    metadata.getTables(catalog, null, null, arrayOf("TABLE")).useEach {
        val tableName = getString("TABLE_NAME")
        tableProviders.add {
            TableMetaData(
                tableName,
                createColumnMetaData(metadata, catalog, tableName)
            )
        }
    }

    return tableProviders.map { it() }
}

private fun createColumnMetaData(metadata: DatabaseMetaData, catalog: String, tableName: String): List<ColumnMetaData> {
    val columns = mutableListOf<ColumnMetaData>()
    metadata.getColumns(catalog, null, tableName, null).useEach {
        val column = ColumnMetaData(
            name = getString("COLUMN_NAME"),
            type = JDBCType.valueOf(getInt("DATA_TYPE")),
            typeName = getString("TYPE_NAME"),
            nullable = getInt("NULLABLE") == 1,
            columnDef = getString("COLUMN_DEF"),
            autoincrement = getString("IS_AUTOINCREMENT")?.let { it == "YES" }
        )
        columns.add(column)
    }
    return columns
}

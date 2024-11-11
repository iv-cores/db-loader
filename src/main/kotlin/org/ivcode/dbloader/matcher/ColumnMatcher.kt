package org.ivcode.dbloader.matcher

import org.ivcode.dbloader.metadata.ColumnMetaData
import org.ivcode.dbloader.metadata.TableMetaData
import java.sql.JDBCType

/**
 * A matcher used to hook into a process if a certain condition is met.
 *
 */
data class ColumnMatcher (
    val dialect: List<String>? = null,
    val table: List<String>? = null,
    val tableIgnoreCase: Boolean = false,
    val column: List<String>? = null,
    val columnIgnoreCase: Boolean = false,
    val type: List<JDBCType>? = null,
    val typeName: List<String>? = null,
    val typeNameIgnoreCase: Boolean = false
) {
    fun isMatch(
        dialect: String,
        table: TableMetaData,
        column: ColumnMetaData
    ): Boolean {
        val isDialectMatch = this.dialect == null || this.dialect.contains(dialect)
        val isTableMatch = this.table == null || this.table.any { it.equals(table.name, ignoreCase = tableIgnoreCase) }
        val isColumnMatch = this.column == null || this.column.any { it.equals(column.name, ignoreCase = columnIgnoreCase) }
        val isTypeMatch = this.type == null || this.type.contains(column.type)
        val isTypeNameMatch = this.typeName == null || this.typeName.any { it.equals(column.typeName, ignoreCase = typeNameIgnoreCase) }

        return isDialectMatch && isTableMatch && isColumnMatch && isTypeMatch && isTypeNameMatch
    }

    fun isMatch(columnMatch: MatchColumn): Boolean =
        isMatch(columnMatch.dialect, columnMatch.table, columnMatch.column)
}

data class MatchColumn (
    val dialect: String,
    val table: TableMetaData,
    val column: ColumnMetaData
)

package org.ivcode.dbloader.typeaddapter

import org.ivcode.dbloader.dialect.Dialect
import org.ivcode.dbloader.matcher.MatchColumn
import org.ivcode.dbloader.matcher.ColumnMatcher
import org.ivcode.dbloader.typeaddapter.adaptlets.IdentityJdbcTypeAdapter
import org.ivcode.dbloader.typeaddapter.adaptlets.PostgresJsonJdbcTypeAdapter
import java.sql.JDBCType

/**
 * Applies a type adapter to a column if the source and target matchers are met.
 */
class TypeAdapter (
    private val columnMatcherAdapterList: List<ColumnMatcherAdapter>
) {

    /**
     * Converts a value from one type to another.
     * If a matching type adaptlet is found, the value is converted, otherwise the value is returned as is.
     *
     * @param value value to convert
     * @param sourceMatch source criteria
     * @param targetCriteria target criteria
     *
     * @return converted value
     */
    fun convert (
        value: Any?,
        sourceMatch: MatchColumn,
        targetMatch: MatchColumn,
    ): Any? {
        val match = columnMatcherAdapterList.firstOrNull {
            it.source.isMatch(sourceMatch) && it.target.isMatch(targetMatch)
        }

        return match?.adapter?.invoke(value) ?: value
    }
}

fun interface JdbcTypeAdapter: (Any?) -> Any?

/**
 * Applies a type adapter to a column if the source and target matchers are met.
 */
class ColumnMatcherAdapter (
    val source: ColumnMatcher = ColumnMatcher(),
    val target: ColumnMatcher = ColumnMatcher(),
    val adapter: JdbcTypeAdapter
)

internal fun defaultColumnMatcherAdapters (): List<ColumnMatcherAdapter> {
    return listOf(
        ColumnMatcherAdapter(
            source = ColumnMatcher (
                // string types
                type = listOf(
                    JDBCType.CHAR, JDBCType.VARCHAR, JDBCType.LONGVARCHAR,
                    JDBCType.NCHAR, JDBCType.NVARCHAR, JDBCType.LONGNVARCHAR
                ),
            ),
            target = ColumnMatcher (
                dialect = listOf(Dialect.POSTGRESQL),
                type = listOf(JDBCType.OTHER),
                typeName = listOf("json")
            ),
            adapter = PostgresJsonJdbcTypeAdapter()
        ),
        ColumnMatcherAdapter(
            adapter = IdentityJdbcTypeAdapter()
        )
    )
}
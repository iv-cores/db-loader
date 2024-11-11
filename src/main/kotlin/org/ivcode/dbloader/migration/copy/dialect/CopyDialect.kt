package org.ivcode.dbloader.migration.copy.dialect

import org.sql2o.Query
import java.sql.ResultSet

interface CopyDialect {

    /**
     * Returns the SQL to disable constraints for a table
     *
     * Example: ALTER TABLE ${table} DISABLE CONSTRAINT ALL
     */
    fun disableConstraints(table: String): SimpleStatement

    /**
     * Returns the SQL to enable constraints for a table
     *
     * Example: ALTER TABLE ${table} ENABLE CONSTRAINT ALL
     */
    fun enableConstraints(table: String): SimpleStatement


    fun selectAll(table: String): SimpleStatement
    fun deleteAll(table: String): SimpleStatement
    fun insert(table: String, columns: List<String>): InsertStatement
    fun updateAutoIncrement(table: String, column: String, value: Int): UpdateAutoIncrementStatement
    fun selectAutoIncrement(table: String, column: String): SelectStatement<Int?>
    fun selectMax(table: String, column: String): SelectStatement<Int?>
}


interface StatementParams  {
    fun with(name: String, value: Any?)
}

fun Query.setStatementParams(params: (StatementParams.() -> Unit)?) = apply {
    if(params == null) {
        return this
    }

    val statementParams = object : StatementParams {
        override fun with(name: String, value: Any?) {
            addParameter(name, value)
        }
    }

    statementParams.params()
}

data class SimpleStatement (
    val statement: String,
    val params: StatementParams.() -> Unit = {}
)

data class InsertStatement (
    val statement: String,
    val params: (StatementParams.(row: Map<String, Any?>) -> Unit)? = null
)

data class UpdateAutoIncrementStatement (
    val statement: String
)

data class SelectStatement<T> (
    val statement: String,
    val parseRow: (ResultSet) -> T
)

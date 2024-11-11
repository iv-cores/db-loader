package org.ivcode.dbloader.migration.copy.dialect.dialects

import org.ivcode.dbloader.migration.copy.dialect.*


class MySqlCopyDialect: CopyDialect {
    override fun disableConstraints(table: String) = SimpleStatement (
        statement = "ALTER TABLE DISABLE CONSTRAINT ALL",
    )

    override fun enableConstraints(table: String) = SimpleStatement (
        statement = "ALTER TABLE ENABLE CONSTRAINT ALL",
    )

    override fun selectAll(table: String) = SimpleStatement (
        statement = "SELECT * FROM `$table`"
    )

    override fun deleteAll(table: String) = SimpleStatement (
        statement = "DELETE FROM `$table`"
    )

    override fun insert (
        table: String,
        columns: List<String>
    ) = InsertStatement (
        statement = "INSERT INTO $table (${columns.joinToString(", ")}) VALUES (${columns.joinToString(", ") { ":${it}" }})",
        params = { row ->
            columns.forEach { column ->
                with(column, row[column])
            }
        }
    )

    override fun updateAutoIncrement(table: String, column: String, value: Int) = UpdateAutoIncrementStatement (
        statement = "ALTER TABLE $table MODIFY COLUMN $column AUTO_INCREMENT = :value"
    )

    override fun selectAutoIncrement(table: String, column: String) = SelectStatement (
        statement = "SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '$table'",
        parseRow = { rs ->
            val value = rs.getInt("AUTO_INCREMENT")
            if(rs.wasNull()) null else value
        }
    )

    override fun selectMax(table: String, column: String) = SelectStatement (
        statement = "SELECT MAX($column) FROM $table",
        parseRow = { rs ->
            val value = rs.getInt(1)
            if(rs.wasNull()) null else value
        }
    )
}


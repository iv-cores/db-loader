package org.ivcode.dbloader.migration.copy.dialect.dialects

import org.ivcode.dbloader.migration.copy.dialect.*

class PostgresCopyDialect: CopyDialect {
    override fun disableConstraints(table: String): SimpleStatement {
        return SimpleStatement("ALTER TABLE \"$table\" DISABLE TRIGGER ALL;")
    }

    override fun enableConstraints(table: String): SimpleStatement {
        return SimpleStatement("ALTER TABLE \"$table\" ENABLE TRIGGER ALL;")
    }

    override fun selectAll(table: String): SimpleStatement {
        return SimpleStatement("SELECT * FROM \"$table\"")
    }

    override fun deleteAll(table: String): SimpleStatement {
        return SimpleStatement("DELETE FROM \"$table\";")
    }

    override fun insert(table: String, columns: List<String>): InsertStatement {
        return InsertStatement(
            statement = "INSERT INTO \"$table\" (\"${columns.joinToString("\", \"")}\") VALUES (${columns.joinToString(", ") { ":$it" }})",
            params = { row ->
                columns.forEach { column ->
                    with(column, row[column])
                }
            }
        )
    }

    override fun updateAutoIncrement(table: String, column: String, value: Int) = UpdateAutoIncrementStatement (
        statement = "ALTER SEQUENCE \"${table}_${column}_seq\" RESTART WITH $value"
    )

    override fun selectAutoIncrement(table: String, column: String) = SelectStatement(
        statement = "SELECT last_value FROM \"${table}_${column}_seq\"",
        parseRow = { rs ->
            val value = rs.getInt("last_value")
            if(rs.wasNull()) null else value
        }
    )

    override fun selectMax(table: String, column: String) = SelectStatement (
        statement = "SELECT MAX(\"$column\") FROM \"$table\"",
        parseRow = { rs ->
            val value = rs.getInt(1)
            if(rs.wasNull()) null else value
        }
    )
}

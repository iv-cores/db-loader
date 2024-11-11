package org.ivcode.dbloader.migration.copy.dialect

import org.ivcode.dbloader.migration.copy.dialect.dialects.MySqlCopyDialect
import org.ivcode.dbloader.migration.copy.dialect.dialects.PostgresCopyDialect

class CopyDialectProvider (
    private val dialects: Map<String, CopyDialect> = mapOf(
        "mysql" to MySqlCopyDialect(),
        "postgres" to PostgresCopyDialect()
    )
) {
    fun getDialect(dialectKey: String): CopyDialect {
        return dialects[dialectKey] ?: throw IllegalArgumentException("Unsupported dialect: $dialectKey")
    }
}
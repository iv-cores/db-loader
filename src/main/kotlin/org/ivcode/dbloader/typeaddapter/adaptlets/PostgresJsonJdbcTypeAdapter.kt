package org.ivcode.dbloader.typeaddapter.adaptlets

import org.ivcode.dbloader.typeaddapter.JdbcTypeAdapter

class PostgresJsonJdbcTypeAdapter: JdbcTypeAdapter {
    override fun invoke(p1: Any?): Any {
        val value = p1?.toString()?.removeSuffix("::json")?.removeSurrounding("'")
        return org.postgresql.util.PGobject().apply {
            type = "json"
            this.value = value
        }
    }
}
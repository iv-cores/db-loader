package org.ivcode.dbloader.typeaddapter.adaptlets

import org.ivcode.dbloader.typeaddapter.JdbcTypeAdapter

class IdentityJdbcTypeAdapter: JdbcTypeAdapter {
    override fun invoke(p1: Any?): Any? {
        return p1
    }
}
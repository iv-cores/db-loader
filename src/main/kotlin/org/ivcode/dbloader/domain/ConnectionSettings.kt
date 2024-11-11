package org.ivcode.dbloader.domain

import javax.sql.DataSource

data class ConnectionSettings (
    val dataSource: DataSource,
    val catalog: String? = null,
)
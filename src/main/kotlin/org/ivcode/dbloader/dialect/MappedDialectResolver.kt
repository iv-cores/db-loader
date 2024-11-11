package org.ivcode.dbloader.dialect

/**
 * Resolves the dialect of a driver.
 */
class MappedDialectResolver(
    private val dialects: Map<String, String> = emptyMap()
): DialectResolver {

    companion object {
        private val DEFAULT_DIALECTS = mapOf(
            "org.postgresql.Driver" to Dialect.POSTGRESQL,
            "com.mysql.cj.jdbc.Driver" to Dialect.MYSQL
        )
    }

    override fun resolve(driver: String): String {
        return dialects[driver] ?: DEFAULT_DIALECTS[driver] ?: throw IllegalArgumentException("Unsupported driver: $driver")
    }
}

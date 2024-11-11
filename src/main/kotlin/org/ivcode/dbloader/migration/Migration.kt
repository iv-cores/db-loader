package org.ivcode.dbloader.migration

import org.ivcode.dbloader.dialect.DialectResolver
import org.ivcode.dbloader.dialect.MappedDialectResolver
import org.ivcode.dbloader.domain.ConnectionSettings
import org.ivcode.dbloader.migration.copy.CopyMigration
import org.ivcode.dbloader.typeaddapter.ColumnMatcherAdapter

/**
 *
 */
class Migration private constructor() {
    companion object {

        /**
         * The copy migration process.
         *
         * This is a data replacement process. The data in the target database will be replaced with the data from the
         * source database.
         *
         * @param dialectResolver overrides the dialect resolver
         * @param source the source connection settings
         * @param target the target connection settings
         * @param typeAdapters adds custom column adapters
         * @param dryRun if true, the transaction will be rolled back after the migration
         */
        fun copy(
            dialectResolver: DialectResolver = MappedDialectResolver(),
            source: ConnectionSettings,
            target: ConnectionSettings,
            typeAdapters: List<ColumnMatcherAdapter>? = null,
            dryRun: Boolean = false
        ) {
            CopyMigration (
                dialectResolver = dialectResolver,
                source = source,
                target = target,
                columnAdapters = typeAdapters,
                dryRun = dryRun
            )()
        }
    }
}

package org.ivcode.dbloader.migration.copy

import org.ivcode.dbloader.utils.DatabaseMapper
import org.ivcode.dbloader.dialect.DialectResolver
import org.ivcode.dbloader.dialect.MappedDialectResolver
import org.ivcode.dbloader.domain.ConnectionSettings
import org.ivcode.dbloader.metadata.ColumnMetaData
import org.ivcode.dbloader.metadata.DatabaseMetaData
import org.ivcode.dbloader.metadata.TableMetaData
import org.ivcode.dbloader.metadata.createDatabaseMetaData
import org.ivcode.dbloader.migration.Migration
import org.ivcode.dbloader.migration.copy.dialect.CopyDialectProvider
import org.ivcode.dbloader.migration.copy.tasks.CopyTableTask
import org.ivcode.dbloader.migration.copy.tasks.DisableConstraintsTask
import org.ivcode.dbloader.migration.copy.tasks.EnableConstraintsTask
import org.ivcode.dbloader.typeaddapter.ColumnMatcherAdapter
import org.ivcode.dbloader.typeaddapter.TypeAdapter
import org.ivcode.dbloader.typeaddapter.defaultColumnMatcherAdapters
import org.ivcode.dbloader.utils.getDriverClassName
import org.ivcode.dbloader.utils.useReadOnly
import org.ivcode.dbloader.utils.useTransaction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.sql2o.Connection
import org.sql2o.Sql2o

/**
 * Copies data from one database to another.
 *
 * This is a replacement plan. The data in the target database will be replaced with the data from the source database.
 */
internal data class CopyMigration (
    val dialectResolver: DialectResolver,
    val source: ConnectionSettings,
    val target: ConnectionSettings,
    val columnAdapters: List<ColumnMatcherAdapter>? = null,
    val dryRun: Boolean = false,
): () -> Unit {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(CopyMigration::class.java)
    }

    override fun invoke() {
        open(this::copy)
    }

    private fun copy(context: CopyContext) {
        val dialectProvider = CopyDialectProvider()
        val copyTasks = createCopyTasks(context, dialectProvider)

        // Run the copy tasks
        copyTasks.forEach { it() }
    }

    private fun createCopyTasks(context: CopyContext, dialectProvider: CopyDialectProvider): List<()->Unit> {
        val taskList = mutableListOf<() -> Unit>()

        val tableMappings = LinkedHashMap<TableMetaData, TableMetaData>()
        context.sourceMetaData.tables.forEach { table ->
            tableMappings[table] = DatabaseMapper.findTargetTable(table, context.targetMetaData)
        }

        // Disable Constraints
        taskList.add(DisableConstraintsTask(context, dialectProvider, tableMappings.keys.toList()))

        // Copy data for each table
        tableMappings.forEach { (sourceTable, targetTable) ->
            val columns = mutableMapOf<ColumnMetaData, ColumnMetaData>()
            sourceTable.columns.forEach { sourceColumn ->
                val targetColumn = DatabaseMapper.findTargetColumn(sourceColumn, targetTable)
                columns[sourceColumn] = targetColumn
            }
            taskList.add(CopyTableTask(context, dialectProvider, sourceTable, targetTable, columns))
        }

        // Enable Constraints
        taskList.add(EnableConstraintsTask(context, dialectProvider, tableMappings.keys.toList()))

        return taskList
    }

    private fun open (func: (CopyContext)->Unit) {
        Sql2o(source.dataSource).open().useReadOnly { sourceConnection ->
            LOGGER.info("Connected to source database")

            Sql2o(target.dataSource).open().useTransaction (dryRun) { targetConnection ->
                LOGGER.info("Connected to target database")

                val sourceDialect = dialectResolver.resolve(getDriverClassName(sourceConnection))
                val targetDialect = dialectResolver.resolve(getDriverClassName(targetConnection))

                func(
                    CopyContext(
                    source = sourceConnection,
                    sourceDialect = sourceDialect,
                    sourceMetaData = createDatabaseMetaData(sourceDialect, source.catalog, sourceConnection),
                    target = targetConnection,
                    targetDialect = dialectResolver.resolve(getDriverClassName(targetConnection)),
                    targetMetaData = createDatabaseMetaData(targetDialect, target.catalog, targetConnection),
                    typeAdapter = createTypeAdapter(columnAdapters)
                )
                )
            }
        }

        LOGGER.info("Migration Process Complete")
    }

    private fun createTypeAdapter(userMatchers: List<ColumnMatcherAdapter>?): TypeAdapter {
        val matchers = userMatchers.orEmpty() + defaultColumnMatcherAdapters()
        return TypeAdapter(matchers)
    }
}

data class CopyContext (
    val source: Connection,
    val sourceDialect: String,
    val sourceMetaData: DatabaseMetaData,

    val target: Connection,
    val targetDialect: String,
    val targetMetaData: DatabaseMetaData,

    val typeAdapter: TypeAdapter
)


package org.ivcode.dbloader.migration.copy.tasks

import org.ivcode.dbloader.metadata.TableMetaData
import org.ivcode.dbloader.migration.copy.CopyContext
import org.ivcode.dbloader.migration.copy.dialect.CopyDialectProvider
import org.ivcode.dbloader.migration.copy.dialect.setStatementParams
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class DisableConstraintsTask (
    private val context: CopyContext,
    private val dialectProvider: CopyDialectProvider,
    private val targetTables: List<TableMetaData>
): () -> Unit {

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(DisableConstraintsTask::class.java)
    }

    override fun invoke() {
        LOGGER.info("Disabling Constraints")

        targetTables.forEach { table ->
            disableConstraints(table.name)
        }
    }

    private fun disableConstraints(table: String) {
        val name = "disable constraints ($table)"

        val targetDialectName = context.targetDialect
        val targetCopyDialect = dialectProvider.getDialect(targetDialectName)

        val statement = targetCopyDialect.disableConstraints(table)

        context.target.createQuery(statement.statement).apply {
            this.name = name
            setStatementParams(statement.params)
            executeUpdate()
        }
    }
}
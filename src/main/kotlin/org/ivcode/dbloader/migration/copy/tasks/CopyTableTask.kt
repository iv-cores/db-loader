package org.ivcode.dbloader.migration.copy.tasks

import org.ivcode.dbloader.matcher.MatchColumn
import org.ivcode.dbloader.metadata.ColumnMetaData
import org.ivcode.dbloader.metadata.TableMetaData
import org.ivcode.dbloader.migration.copy.CopyContext
import org.ivcode.dbloader.migration.copy.dialect.CopyDialectProvider
import org.ivcode.dbloader.migration.copy.dialect.setStatementParams
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.ResultSet

internal class CopyTableTask(
    private val context: CopyContext,
    private val copyDialectProvider: CopyDialectProvider,
    private val sourceTable: TableMetaData,
    private val targetTable: TableMetaData,
    private val columnMap: Map<ColumnMetaData, ColumnMetaData>,
    private val batchSize: Int = 100
): () -> Unit {

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(CopyTableTask::class.java)
    }

    override fun invoke() {
        deleteTargetData()
        copyRowData()
        setAutoIncrements()
    }

    private fun deleteTargetData() {
        val name = "delete all (${targetTable.name})"

        val targetDialectName = context.targetDialect
        val targetCopyDialect = copyDialectProvider.getDialect(targetDialectName)

        val statement = targetCopyDialect.deleteAll(targetTable.name)

        val updatedRows = context.target.createQuery(statement.statement).use {
            it.name = name
            it.setStatementParams(statement.params).executeUpdate().result
        }

        LOGGER.debug("rows updated: $updatedRows [$name]")
    }

    private fun copyRowData() {
        val sourceDialectName = context.sourceDialect
        val sourceCopyDialect = copyDialectProvider.getDialect(sourceDialectName)

        val targetDialectName = context.targetDialect
        val targetCopyDialect = copyDialectProvider.getDialect(targetDialectName)

        val targetInsertStatement = targetCopyDialect.insert(targetTable.name, columnMap.map { it.value.name })
        context.target.createQuery(targetInsertStatement.statement).apply {
            maxBatchRecords = batchSize
            name = "Copy Table: ${sourceTable.name} -> ${targetTable.name}"
        }.use { targetQuery ->
            val sourceStatement = sourceCopyDialect.selectAll(sourceTable.name)
            context.source.createQuery(sourceStatement.statement)
                .setStatementParams(sourceStatement.params)
                .executeAndFetch { rs: ResultSet ->
                    val row = columnMap.entries.associate { (sourceColumn, targetColumn) ->
                        var value = rs.getObject(sourceColumn.name)

                        if(value == null && targetColumn.columnDef!=null) {
                            value = targetColumn.columnDef
                        }

                        val typeAdapter = context.typeAdapter
                        val transformedValue = typeAdapter.convert(
                            value = value,
                            sourceMatch = MatchColumn(sourceDialectName, sourceTable, sourceColumn),
                            targetMatch = MatchColumn(targetDialectName, targetTable, targetColumn)
                        )

                        targetColumn.name to transformedValue
                    }

                    targetQuery
                        .setStatementParams { targetInsertStatement.params?.invoke(this, row) }
                        .addToBatch()
                }

            if(targetQuery.currentBatchRecords > 0) {
                val count = targetQuery.executeBatch().result
                LOGGER.debug("Copied $count rows")
            }

        }
    }

    private fun setAutoIncrements() {
        columnMap.entries.forEach { (sourceColumn, targetColumn) ->
            if(sourceColumn.autoincrement==true && targetColumn.autoincrement==true) {
                setAutoIncrement(sourceColumn, targetColumn)
            }
        }
    }

    private fun setAutoIncrement(sourceColumn: ColumnMetaData, targetColumn: ColumnMetaData) {
        LOGGER.debug("TODO: Copy Auto Increment: ${sourceColumn.name} -> ${targetColumn.name}")

        val isTargetAutoIncrement = targetColumn.autoincrement
        if(isTargetAutoIncrement == false) {
            // no need to copy auto increment
            return
        }

        val isSourceAutoIncrement = sourceColumn.autoincrement

        if(isSourceAutoIncrement == true) {
            copyAutoIncrement(sourceColumn, targetColumn)
        } else {
            setAutoIncrementToMax(targetColumn)
        }
    }

    private fun copyAutoIncrement(sourceColumn: ColumnMetaData, targetColumn: ColumnMetaData) {
        val name = "update auto increment copy (${sourceColumn.name} -> ${targetColumn.name})"

        val sourceDialectName = context.sourceDialect
        val sourceCopyDialect = copyDialectProvider.getDialect(sourceDialectName)

        val targetDialectName = context.targetDialect
        val targetCopyDialect = copyDialectProvider.getDialect(targetDialectName)

        val sourceAutoIncrementStatement = sourceCopyDialect.selectAutoIncrement(sourceTable.name, sourceColumn.name)
        val autoIncrement = context.source.createQuery(sourceAutoIncrementStatement.statement).use {
            it.name = name
            it.executeAndFetchFirst { rs: ResultSet ->
                sourceAutoIncrementStatement.parseRow(rs)
            }
        } ?: throw IllegalStateException("Auto Increment not found for ${sourceTable.name}.${sourceColumn.name}")

        val targetStatement = targetCopyDialect.updateAutoIncrement(targetTable.name, targetColumn.name, autoIncrement)
        context.target
            .createQuery(targetStatement.statement)
            .setName(name)
            .executeUpdate()

        LOGGER.debug("Auto Increment: $autoIncrement [$name]")
    }

    private fun setAutoIncrementToMax(targetColumn: ColumnMetaData) {
        val name = "update auto increment max (${targetColumn.name})"

        val targetDialectName = context.targetDialect
        val targetDialect = copyDialectProvider.getDialect(targetDialectName)

        val maxStatement = targetDialect.selectMax(targetTable.name, targetColumn.name)
        val max = context.target.createQuery(maxStatement.statement).use {
            it.name = name
            it.executeAndFetchFirst { rs: ResultSet ->
                maxStatement.parseRow(rs)
            }
        } ?: 0
        val autoIncrement = max + 1

        val targetStatement = targetDialect.updateAutoIncrement(targetTable.name, targetColumn.name, autoIncrement)
        context.target
            .createQuery(targetStatement.statement)
            .setName(name)
            .executeUpdate()

        LOGGER.debug("Auto Increment: $autoIncrement [$name]")
    }
}
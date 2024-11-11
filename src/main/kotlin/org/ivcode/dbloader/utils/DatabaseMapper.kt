package org.ivcode.dbloader.utils

import org.ivcode.dbloader.metadata.ColumnMetaData
import org.ivcode.dbloader.metadata.DatabaseMetaData
import org.ivcode.dbloader.metadata.TableMetaData

/**
 * Maps a source database to a target database.
 */
class DatabaseMapper private constructor() {
    companion object {
        /**
         * Maps a source database to a target database.
         */
        fun findTargetTable (
            sourceTable: TableMetaData,
            targetDatabase: DatabaseMetaData
        ): TableMetaData {

            // exact match
            var table: TableMetaData? = null
            targetDatabase.tables.forEach {
                if (it.name == sourceTable.name) {
                    require(table == null) { "Multiple tables with the same name: ${sourceTable.name}" }
                    table = it
                }
            }
            if(table != null) {
                return table!!
            }

            // case-insensitive match
            targetDatabase.tables.forEach {
                if (it.name.equals(sourceTable.name, ignoreCase = true)) {
                    require(table == null) { "Multiple tables with the same name: ${sourceTable.name}" }
                    table = it
                }
            }
            if(table != null) {
                return table!!
            }

            throw IllegalArgumentException("Table not found: ${sourceTable.name}")
        }

        /**
         * Maps a source column to a target column.
         */
        fun findTargetColumn (
            sourceColumn: ColumnMetaData,
            targetTableMetaData: TableMetaData
        ): ColumnMetaData {
            // exact match
            var column: ColumnMetaData? = null
            targetTableMetaData.columns.forEach {
                if (it.name == sourceColumn.name) {
                    require(column == null) { "Multiple columns with the same name: ${sourceColumn.name}" }
                    column = it
                }
            }
            if(column != null) {
                return column!!
            }

            // case-insensitive match
            targetTableMetaData.columns.forEach {
                if (it.name.equals(sourceColumn.name, ignoreCase = true)) {
                    require(column == null) { "Multiple columns with the same name: ${sourceColumn.name}" }
                    column = it
                }
            }
            if(column != null) {
                return column!!
            }

            throw IllegalArgumentException("Column not found: ${targetTableMetaData.name}.${sourceColumn.name}")
        }
    }
}
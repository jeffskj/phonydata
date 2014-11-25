package io.github.phonydata.writer

import groovy.sql.Sql
import io.github.phonydata.DataSet
import io.github.phonydata.Table

import java.sql.DatabaseMetaData

import javax.sql.DataSource

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DatabaseDataSetWriter implements DataSetWriter {
    private static Logger logger = LoggerFactory.getLogger(DatabaseDataSetWriter)
    
    private Sql sql
    private String quote
    private boolean clean
    
    DatabaseDataSetWriter(DataSource dataSource, boolean clean) {
        sql = new Sql(dataSource)
        quote = dataSource.connection.metaData.identifierQuoteString
        this.clean = clean
    }
    
    public void write(DataSet ds) {
        def fks = findForeignKeys(ds.tables.keySet())
        def sortedTables = topologicalSort(fks)
        def originalNames = ds.tables.keySet().collectEntries { [it.toLowerCase(), it] }
        
        sql.withTransaction{
            sql.withBatch {
                if (clean) {
                    logger.info("deleting data from table {}", originalNames[it])
                    sortedTables.reverse().each {
                        sql.executeUpdate('delete from ' + originalNames[it])
                    }
                }
                
                sortedTables.each { tableName ->
                    sql.cacheStatements { 
                        def table = ds.tables[originalNames[tableName]]
                        def insertStatement = createInsertStatement(table)
                        table.each {
                            sql.executeInsert(insertStatement, it.data);
                        }
                        
                        logger.info("inserted {} rows into {}", table.rows.size(), table.name)
                    }
                }    
            }
        }
        
    }

    String createInsertStatement(Table t) {
        def cols = t.columns.toList()
        def quotedCols = cols.collect { q(it) }
        def paramCols = cols.collect { ':' + it }
        return "insert into ${t.name} (${quotedCols.join(',')}) values (${paramCols.join(',')});".toString()
    }
    
    private String q(String s) {
        return quote + s.toUpperCase() + quote
    }
    
    static def topologicalSort(Map fks) {
        def sorted = []
        def counter = fks.size()
        while (!fks.isEmpty()) {
            if (counter-- < 0) { //TODO: consider warning then just throwing the rest in the sorted list
                logger.error("unable to sort dataset with circular table references: {}", fks)
                throw new IllegalStateException("unable to topologically sort cyclical table references!")
            }
            def unReferenced = fks.findAll { t, refs -> refs.isEmpty() }.keySet()
            sorted.addAll(unReferenced)
            unReferenced.each { fks.remove(it) }
            fks.each { t, refs -> refs.removeAll(unReferenced) }
        }
        return sorted
    }
    
    private Map findForeignKeys(tableNames) {
        def fks = tableNames.collectEntries { [it.toLowerCase(), [] as Set] }
        def metaData = sql.dataSource.connection.metaData
        tableNames.each { addFksForTable(metaData, it, fks) }
        return fks
    }
    
    private void addFksForTable(DatabaseMetaData metaData, tableName, fks) {
        def rs = metaData.getExportedKeys(null, null, tableName.toUpperCase())
        
        while (rs.next()) {
            def dependency = rs.toRowResult()['FKTABLE_NAME'].toLowerCase()
            if (fks[dependency] == null) {
                logger.debug("skipping fk reference to unknown table {} -> {}", tableName, dependency) 
                continue 
            }
            fks[dependency].add(tableName.toLowerCase())
        }
    }
}

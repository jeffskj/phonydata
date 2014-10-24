package io.github.phonydata

import groovy.sql.Sql

import java.sql.Connection
import java.sql.DatabaseMetaData

class DatabaseDataSetWriter implements DataSetWriter {
    private Sql sql
    
    DatabaseDataSetWriter(Connection conn) {
        sql = new Sql(conn)
    }
    
    public void write(DataSet ds) {
        def fks = findForeignKeys(ds.tables.keySet())
        def sortedTables = topologicalSort(fks) 
    }

    static def topologicalSort(Map fks) {
        def sorted = []
        def counter = fks.size()
        while (!fks.isEmpty()) {
            if (counter-- < 0) {
                throw new IllegalStateException("unable to topologically sort cyclical table references!")
            }
            def unReferenced = fks.findAll { t, refs -> refs.isEmpty() }.keySet()
            sorted.addAll(unReferenced)
            unReferenced.each { fks.remove(it) }
            fks.each { t, refs -> refs.removeAll(unReferenced) }
        }
        return sorted
    }
    
    private Map findForeignKeys(tables) {
        def metaData = sql.connection.metaData
        return tables.collectEntries {
            [it, getFksForTable(metaData, it)]
        }
    }
    
    private List getFksForTable(DatabaseMetaData metaData, tableName) {
        def rs = metaData.getImportedKeys(null, null, tableName)
        def fks = [] as Set
        
        while (rs.next()) {
            fks << rs.toRowResult()['FKTABLE_NAME']
        }
        return fks
    }
}

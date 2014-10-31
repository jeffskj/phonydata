package io.github.phonydata.reader

import groovy.sql.Sql
import io.github.phonydata.DataSet
import io.github.phonydata.Table

import java.sql.ResultSet

import javax.sql.DataSource

class DatabaseDataSetReader implements DataSetReader {

    private DataSource ds
    private Set<String> tablesToImport 
    
    DatabaseDataSetReader(DataSource ds) {
        this.ds = ds
    }
    
    DatabaseDataSetReader(DataSource ds, Collection<String> tablesToImport) {
        this.ds = ds
        this.tablesToImport = new HashSet(tablesToImport)
    }
    
    public DataSet read() {
        if (!tablesToImport) {
            tablesToImport = findAllTables()
        }
        
        def queries = tablesToImport.collectEntries { [it, "select * from $it".toString()] }
        
        def dataSet = new DataSet(tables: [:])
        
        def sql = new Sql(ds)
        sql.withBatch { 
            queries.each { tableName, query ->
                Table table = new Table(name: tableName)
                
                sql.rows(query).each {
                    table.addRow(it)
                }
                
                dataSet.tables[tableName] = table
            }
        }
        
        return dataSet;
    }
    
    private Set<String> findAllTables() {
        def conn = ds.connection
        ResultSet rs = conn.metaData.getTables(conn.catalog, conn.schema, "%", null);
        
        def tables = [] as Set 
        
        while (rs.next()) {
          tables << rs.getString(3)
        }
        
        return tables
    }
}

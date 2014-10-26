package io.github.phonydata

import groovy.sql.Sql

import java.sql.DatabaseMetaData

import javax.sql.DataSource

class DatabaseDataSetWriter implements DataSetWriter {
    private Sql sql
    private String quote
    private Set<String> keywords
    
    DatabaseDataSetWriter(DataSource dataSource) {
        sql = new Sql(dataSource)
        quote = dataSource.connection.metaData.identifierQuoteString
        keywords = getKeywords(dataSource.connection.metaData)
    }
    
    public void write(DataSet ds) {
        def fks = findForeignKeys(ds.tables.keySet())
        println fks
        def sortedTables = topologicalSort(fks)
        println sortedTables
        sql.withTransaction{
            sql.withBatch {
                sortedTables.each { tableName ->
                    sql.cacheStatements { 
                        def table = ds.tables[tableName]
                        def insertStatement = createInsertStatement(table)
                        table.each {
                            sql.executeInsert(insertStatement, it.data);
                        }
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
        if (keywords.contains(s)) {
            return quote + s + quote
        }
        return s
    }
    
    static def topologicalSort(Map fks) {
        def sorted = []
        def counter = fks.size()
        while (!fks.isEmpty()) {
            if (counter-- < 0) { //TODO: consider warning then just throwing the rest in the sorted list
                throw new IllegalStateException("unable to topologically sort cyclical table references!")
            }
            def unReferenced = fks.findAll { t, refs -> refs.isEmpty() }.keySet()
            sorted.addAll(unReferenced)
            unReferenced.each { fks.remove(it) }
            fks.each { t, refs -> refs.removeAll(unReferenced) }
        }
        return sorted
    }
    
    static def getKeywords(DatabaseMetaData md) {
        def keywords = [] as Set
        keywords.addAll(md.SQLKeywords.split(','))
        keywords.addAll(SQL_2K3_KEYWORDS)
        return keywords
        
    }
    
    private Map findForeignKeys(tables) {
        def fks = tables.collectEntries { [it.toLowerCase(), [] as Set] }
        def metaData = sql.dataSource.connection.metaData
        tables.each { addFksForTable(metaData, it, fks) }
        return fks
    }
    
    private void addFksForTable(DatabaseMetaData metaData, tableName, fks) {
        def rs = metaData.getExportedKeys(null, null, tableName.toUpperCase())
        
        while (rs.next()) {
            def dependency = rs.toRowResult()['FKTABLE_NAME'].toLowerCase()
            fks[dependency].add(tableName)
        }
    }
    
    private static final Set<String> SQL_2K3_KEYWORDS = [
        'ADD','ALL','ALLOCATE','ALTER','AND','ANY','ARE','ARRAY','AS','ASENSITIVE','ASYMMETRIC','AT','ATOMIC',
        'AUTHORIZATION','BEGIN','BETWEEN','BIGINT','BINARY','BLOB','BOOLEAN','BOTH','BY','CALL','CALLED','CASCADED',
        'CASE','CAST','CHAR','CHARACTER','CHECK','CLOB','CLOSE','COLLATE','COLUMN','COMMIT','CONNECT','CONSTRAINT',
        'CONTINUE','CORRESPONDING','CREATE','CROSS','CUBE','CURRENT','CURRENT_DATE','CURRENT_DEFAULT_TRANSFORM_GROUP',
        'CURRENT_PATH','CURRENT_ROLE','CURRENT_TIME','CURRENT_TIMESTAMP','CURRENT_TRANSFORM_GROUP_FOR_TYPE','CURRENT_USER',
        'CURSOR','CYCLE','DATE','DAY','DEALLOCATE','DEC','DECIMAL','DECLARE','DEFAULT','DELETE','DEREF','DESCRIBE',
        'DETERMINISTIC','DISCONNECT','DISTINCT','DOUBLE','DROP','DYNAMIC','EACH','ELEMENT','ELSE','END','END','EXEC',
        'ESCAPE','EXCEPT','EXEC','EXECUTE','EXISTS','EXTERNAL','FALSE','FETCH','FILTER','FLOAT','FOR','FOREIGN','FREE','FROM',
        'INNER','INOUT','INPUT','INSENSITIVE','INSERT','INT','INTEGER','INTERSECT','INTERVAL','INTO','IS','ISOLATION','JOIN',
        'LANGUAGE','LARGE','LATERAL','LEADING','LEFT','LIKE','LOCAL','LOCALTIME','LOCALTIMESTAMP','MATCH','MEMBER','MERGE',
        'METHOD','MINUTE','MODIFIES','MODULE','MONTH','MULTISET','NATIONAL','NATURAL','NCHAR','NCLOB','NEW','NO','NONE','NOT',
        'NULL','NUMERIC','OF','OLD','ON','ONLY','OPEN','OR','ORDER','OUT','OUTER','OUTPUT','OVER','OVERLAPS','PARAMETER',
        'PARTITION','PRECISION','PREPARE','PRIMARY','PROCEDURE','RANGE','READS','REAL','RECURSIVE','REF','REFERENCES',
        'REFERENCING','REGR_AVGX','REGR_AVGY','REGR_COUNT','REGR_INTERCEPT','REGR_R2','REGR_SLOPE','REGR_SXX','REGR_SXY',
        'REGR_SYY','RELEASE','RESULT','RETURN','RETURNS','REVOKE','RIGHT','ROLLBACK','ROLLUP','ROW','ROWS','SAVEPOINT','SCROLL',
        'SEARCH','SECOND','SELECT','SENSITIVE','SESSION_USER','SET','SIMILAR','SMALLINT','SOME','SPECIFIC','SPECIFICTYPE','SQL',
        'SQLEXCEPTION','SQLSTATE','SQLWARNING','START','STATIC','SUBMULTISET','SYMMETRIC','SYSTEM','SYSTEM_USER','TABLE','THEN',
        'TIME','TIMESTAMP','TIMEZONE_HOUR','TIMEZONE_MINUTE','TO','TRAILING','TRANSLATION','TREAT','TRIGGER','TRUE','UESCAPE','UNION',
        'UNIQUE','UNKNOWN','UNNEST','UPDATE','UPPER','USER','USING','VALUE','VALUES','VAR_POP','VAR_SAMP','VARCHAR','VARYING','WHEN',
        'WHENEVER','WHERE','WIDTH_BUCKET','WINDOW','WITH','WITHIN','WITHOUT','YEAR'] as Set
}

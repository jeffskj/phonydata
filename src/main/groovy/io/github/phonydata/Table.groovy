package io.github.phonydata


class Table implements Iterable<Row> {
    String name
    private String idColumn
    private long nextId = -1
    
    private Map<String, Object> defaults = [:]
    
    private final List<Row> rows = []
    private final Set<String> columns = [] as Set
    
    public List<Row> getRows() {
        return rows
    }
    
    public Row addRow(Map<String, Object> row) {
        columns.addAll(row.keySet())
        
        if (!row[idColumn] && nextId > 0) {
            row[idColumn] = nextId++
        } else if (row[idColumn] && row[idColumn] > nextId) {
            nextId = row[idColumn]+1
        }
        
        def r = new Row(table: this, data: row)
        rows.add(r)
        return r
    }
    
    public void defaults(Map<String, Object> defaults) {
        this.defaults = defaults
        this.columns.addAll(defaults.keySet())
    }
    
    public void id(String col, boolean auto = true) {
        idColumn = col
        columns.add(col)
        
        if (auto) {
            nextId = 1
        }
    }
    
    Map<String, Object> getDefaults() { defaults }

    public Iterator<Row> iterator() {
        return rows.iterator();
    }
}

class Row {
    Table table
    Map<String, Object> data
    
    Set<String> getColumns() { table.columns } 
    def propertyMissing(name) { val(data[name] ?: table.defaults[name]) }
    
    def propertyMissing(name, value) { 
        data[name] = value
        columns.add(name) 
    }
    
    Map<String, Object> getData() {
        return columns.collectEntries { [it, propertyMissing(it)] }
    }
    
    private def val(v) {
        if (v instanceof Row) {
            if (v.table == null || v.table.idColumn == null) {
                throw new IllegalStateException('table with idColumn must be set to pass row directly')
            }
            return v.data[v.table.idColumn]
        }
        
        if (v instanceof Closure) {
            return v()
        }
        
        return v
    }
    
    @Override
    public String toString() {
        return getData().toString();
    }
}
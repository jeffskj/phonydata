package io.github.phonydata.writer

import io.github.phonydata.DataSet;
import io.github.phonydata.Row;
import io.github.phonydata.Table;



class GroovyDataSetWriter implements DataSetWriter {
    
    private PrintWriter out;

    GroovyDataSetWriter(OutputStream out) {
        this.out = out.newPrintWriter()
    }
    
    GroovyDataSetWriter(Writer out) {
        this.out = out.newPrintWriter();
    }
    
    public void write(DataSet ds) {
        ds.tables.values().each { Table t ->
            t.rows.each { Row r ->
                def cols = r.data.findAll { it.value != null }.collect { n, v -> "${n}:${toValue(v)}" }.join(',')
                out.println("${t.name}(${cols})")
            }        
            out.println()
        }
    }
    
    private String toValue(Object o) {
        switch (o) {
            case Number: return o.toString()
            case CharSequence: return "'$o'"
            case Boolean: return o.toString() 
            case Date: return "new Date(${o.time})"
        }
    }
}

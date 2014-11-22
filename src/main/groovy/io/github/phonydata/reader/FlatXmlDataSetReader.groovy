package io.github.phonydata.reader

import io.github.phonydata.DataSet
import io.github.phonydata.Table

import java.text.ParseException

class FlatXmlDataSetReader implements DataSetReader {
    private InputStream input;
    
    FlatXmlDataSetReader(String text) {
        this(new ByteArrayInputStream(text.bytes))
    }
    
    FlatXmlDataSetReader(InputStream input) {
        this.input = input
    }
    
    public DataSet read() {
        return new DataSet(tables: parse(input));
    }

    private Map<String, Table> parse(InputStream input) {
        def tables = [:]
        
        def getTable = { name ->
            if (!tables[name]) {
                tables[name] = new Table(name: name)
            }
            return tables[name]
        }
        
        def xml = new XmlSlurper().parse(input)
        
        xml.children().each {
            getTable(it.name()).addRow(it.attributes().collectEntries { n, v -> [n, convert(v)] })
        }
        
        return tables
    }
    
    private def convert(input) {
        def dateFormats = ['yyyy-MM-dd HH:mm:ss.SSSS': java.sql.Timestamp,
                           'yyyy-MM-dd': java.sql.Date,
                           'hh:mm:ss': java.sql.Time]
        
        def parsedDate = dateFormats.collect { format, type -> 
            try {
                return type.newInstance(Date.parse(format, input).time)
            } catch (ParseException e) {
                return null
            }
        }.find { it != null }
        
        if (parsedDate != null) return parsedDate
        
        if (input ==~ /-?\d+/) {
            return Long.parseLong(input)
        }
        
        if (input ==~ /-?\d+\.\d+/) {
            return Double.parseDouble(input)
        }
        
        return input
    }
}

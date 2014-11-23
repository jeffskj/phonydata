package io.github.phonydata;

import io.github.phonydata.reader.DataSetReader
import io.github.phonydata.reader.DatabaseDataSetReader
import io.github.phonydata.reader.FlatXmlDataSetReader
import io.github.phonydata.reader.GroovyClosureDataSetReader
import io.github.phonydata.reader.GroovyDataSetReader
import io.github.phonydata.writer.DatabaseDataSetWriter
import io.github.phonydata.writer.GroovyDataSetWriter

import javax.sql.DataSource

public class PhonyData {

    private DataSetReader reader
    
    public static PhonyData from(Closure clos) {
        return new PhonyData(reader: new GroovyClosureDataSetReader(clos)) 
    }
    
    public static PhonyData from(String groovyDataset) {
        return new PhonyData(reader: new GroovyDataSetReader(groovyDataset))
    }
    
    public static PhonyData from(InputStream groovyDataset) {
        return new PhonyData(reader: new GroovyDataSetReader(groovyDataset))
    }
    
    public static PhonyData fromXml(String flatXml) {
        return new PhonyData(reader: new FlatXmlDataSetReader(flatXml))
    }
    
    public static PhonyData fromXml(InputStream flatXml) {
        return new PhonyData(reader: new FlatXmlDataSetReader(flatXml))
    }
    
    public static PhonyData from(DataSource ds, Collection<String> tables) {
        return new PhonyData(reader: new DatabaseDataSetReader(ds, tables))
    }
    
    public void into(DataSource dest) {
        into(dest, false)
    }

    public void into(DataSource dest, boolean clean) {
        def writer = new DatabaseDataSetWriter(dest, clean)
        writer.write(reader.read())
    }

    public void into(OutputStream dest) {
        def writer = new GroovyDataSetWriter(dest)
        writer.write(reader.read())
    }
}
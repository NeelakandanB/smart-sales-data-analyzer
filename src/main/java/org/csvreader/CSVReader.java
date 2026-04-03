package org.csvreader;

public interface CSVReader
{
    void read();

    static CSVReader getInstance()
    {
        return new CSVFileReader();
    }
}

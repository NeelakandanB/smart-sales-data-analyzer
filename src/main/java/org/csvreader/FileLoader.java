package org.csvreader;

import com.sun.jdi.connect.spi.Connection;
import org.Exceptions.InvalidRequestException;

import java.io.File;
import java.util.Scanner;

public class FileLoader
{
    private static final Scanner scan  = new Scanner(System.in);

    public void load()
    {
        System.out.print("Enter your CSV File Path : ");

        try
        {
            String filePath = scan.nextLine();
            File file = new File( filePath );

            if( !file.exists() || !file.isFile() )
            {
                throw new InvalidRequestException( "Invalid file Path , Enter a valid file Path " );
            }
            else
            {
                CSVReader reader = CSVReader.getInstance();
                reader.read();
            }

        }
        catch ( Exception e )
        {
            System.out.println(e.getMessage());
        }
    }
}

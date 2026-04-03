package org;

import java.util.Scanner;

public class Main
{
    private static final Scanner scan = new Scanner( System.in );

    public static void main( String[] args )
    {
        System.out.println( "=== Welcome to Smart Data Analyzer === " );
        new Main().init();
    }

    private void init()
    {
        while ( true )
        {
            System.out.println( " === Choose one option from the menu ===" );
            System.out.println( "1.Load CSV file" );
            System.out.println( "2.Exit" );

            try
            {
                System.out.print( "Enter your Choice : " );
                int choice = scan.nextInt();
                scan.nextLine();

                switch ( choice )
                {
                    case 1:
                    {

                        break;
                    }
                    case 2:
                    {
                        System.out.println( "Exit Successful..." );
                        System.exit( 0 );
                    }
                    default:
                    {
                        System.out.println( "Invalid choice !! choose hte correct Choice " );
                    }
                }
            }
            catch ( Exception e )
            {
                System.out.println( "Invalid choice !! choose hte correct Choice " );
                scan.nextLine();
            }
        }
    }
}

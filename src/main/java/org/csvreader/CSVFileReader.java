package org.csvreader;

import org.util.ColumnMapper;

import java.util.Scanner;
import java.io.*;
import java.util.*;
import java.util.logging.*;


public class CSVFileReader
{

    private static final Logger LOGGER =
            Logger.getLogger( CSVReader.class.getName() );

    private static final String CSV_EXTENSION = ".csv";

    // order_date is index 0 in the standard schema — critical for all queries
    private static final int STANDARD_DATE_INDEX = 0;

    // ─── Public API ────────────────────────────────────────────────────────────

    /**
     * Reads a CSV file and returns clean, standardized data rows.
     * <p>
     * Flow:
     * 1. Validate file path and accessibility
     * 2. Open file with BufferedReader (try-with-resources)
     * 3. Read header → pass to ColumnMapper to get mapping
     * 4. For each data row: validate → apply mapping → collect
     * 5. Print audit summary
     * 6. Return list of standardized rows
     *
     * @param filePath Absolute or relative path to the CSV file
     * @return List of String[] rows in STANDARD_COLUMNS order.
     * Never null. Never empty (throws if empty).
     * @throws FileNotFoundException    if file does not exist at given path
     * @throws IllegalArgumentException if file is not a .csv file,
     *                                  or file is not readable
     * @throws IOException              if file I/O fails mid-read,
     *                                  or column mapping wizard fails
     * @throws Exception                if no valid rows found after full read
     */
    public static List<String[]> readCSV( String filePath ) throws Exception
    {

        // ── Phase 1: Pre-read validation ───────────────────────────────────────
        validateFilePath( filePath );

        // ── Phase 2: Read and process the file ────────────────────────────────
        List<String[]> validRows = new ArrayList<>();
        List<String> skippedRows = new ArrayList<>();

        // try-with-resources: BufferedReader is ALWAYS closed, even on exception
        // This prevents file handle leaks which silently accumulate and
        // cause "Too many open files" errors in production
        try ( BufferedReader br = new BufferedReader( new FileReader( filePath ) ) )
        {

            String line = null;
            int lineNumber = 0;
            boolean isHeader = true;
            int[] mapping = null;

            while ( ( line = br.readLine() ) != null )
            {
                lineNumber++;

                // Skip blank lines — common at end of CSV files
                if ( line.trim().isEmpty() )
                {
                    LOGGER.fine( "Skipping blank line at line " + lineNumber );
                    continue;
                }

                String[] cols = line.split( ",", - 1 );
                // Note: split(",", -1) keeps trailing empty strings.
                // split(",") without -1 drops trailing commas silently —
                // which would cause off-by-one errors on rows ending with ","

                // ── Header row ────────────────────────────────────────────────
                if ( isHeader )
                {
                    isHeader = false;

                    if ( cols.length == 0 )
                    {
                        throw new IOException(
                                "Header row is empty at line 1. " +
                                        "CSV file must have a header row." );
                    }

                    System.out.println( "✅ Header detected — " +
                            cols.length + " columns: " + line );

                    // Run column mapping (interactive wizard or load saved)
                    try
                    {
                        mapping = ColumnMapper.getMapping( cols );
                    }
                    catch ( IOException e )
                    {
                        // Mapping failed (e.g. user exceeded retries)
                        // Wrap with context before rethrowing
                        throw new IOException(
                                "Column mapping failed and cannot be recovered. " +
                                        "Cause: " + e.getMessage(), e );
                    }

                    System.out.println( "📂 Starting data ingestion...\n" );
                    continue;
                }

                // ── Guard: mapping must exist before processing data rows ──────
                // This should never be null here (header always comes first),
                // but defensive check prevents NullPointerException if file
                // has no header row at all
                if ( mapping == null )
                {
                    skippedRows.add( buildSkipMessage( lineNumber, line,
                            "no column mapping available — header may be missing" ) );
                    continue;
                }

                // ── Data row validation ───────────────────────────────────────
                String skipReason = validateDataRow( cols, mapping, lineNumber );
                if ( skipReason != null )
                {
                    skippedRows.add( buildSkipMessage( lineNumber, line, skipReason ) );
                    LOGGER.warning( "Skipped line " + lineNumber + ": " + skipReason );
                    continue;
                }

                // ── Apply column mapping → standardized row ───────────────────
                String[] standardRow;
                try
                {
                    standardRow = ColumnMapper.applyMapping( cols, mapping );
                }
                catch ( IllegalArgumentException e )
                {
                    // applyMapping guards against null — this should never fire,
                    // but if it does, skip the row and log it
                    skippedRows.add( buildSkipMessage( lineNumber, line,
                            "mapping error: " + e.getMessage() ) );
                    LOGGER.severe( "Unexpected mapping error at line " +
                            lineNumber + ": " + e.getMessage() );
                    continue;
                }

                // ── Final check: order_date must not be empty after mapping ───
                if ( standardRow[ STANDARD_DATE_INDEX ].isEmpty() )
                {
                    skippedRows.add( buildSkipMessage( lineNumber, line,
                            "order_date is empty after mapping — cannot insert without date" ) );
                    continue;
                }

                validRows.add( standardRow );
            }

        }
        catch ( FileNotFoundException e )
        {
            // File disappeared between our validation check and the open
            // (race condition — rare but possible on network drives)
            throw new FileNotFoundException(
                    "CSV file not found (may have been moved or deleted): " +
                            filePath );

        }
        catch ( IOException e )
        {
            // Mid-read I/O failure — disk error, network drive disconnect, etc.
            throw new IOException(
                    "Failed to read CSV file: " + filePath +
                            ". Cause: " + e.getMessage(), e );
        }

        // ── Phase 3: Audit summary ─────────────────────────────────────────────
        printAuditSummary( validRows.size(), skippedRows );

        // ── Phase 4: Fail fast if no usable data ──────────────────────────────
        if ( validRows.isEmpty() )
        {
            throw new Exception(
                    "No valid data rows found in: " + filePath + ". " +
                            "All rows were skipped. Check the audit summary above." );
        }

        return validRows;
    }

    /**
     * Prints a formatted preview of loaded data.
     * Used for Day 2 testing. Will be removed in the final menu-driven version.
     *
     * @param rows List returned by readCSV()
     */
    public static void printRows( List<String[]> rows )
    {
        if ( rows == null || rows.isEmpty() )
        {
            System.out.println( "⚠️  No rows to display." );
            return;
        }

        System.out.println( "\n📋 Data Preview (first 5 rows):" );
        System.out.printf( "%-12s %-20s %-15s %-8s %-6s %-12s%n",
                "Date", "Product", "Category", "Region", "Qty", "Total(₹)" );
        System.out.println( "─".repeat( 78 ) );

        int count = Math.min( rows.size(), 5 );
        for ( int i = 0 ; i < count ; i++ )
        {
            String[] row = rows.get( i );

            // Guard: row must be long enough
            if ( row.length < 7 )
            {
                System.out.println( "   ⚠️  Row " + ( i + 1 ) +
                        " has fewer columns than expected. Skipping preview." );
                continue;
            }

            System.out.printf( "%-12s %-20s %-15s %-8s %-6s %-12s%n",
                    row[ 0 ],  // order_date
                    row[ 1 ],  // product_name
                    row[ 2 ],  // category
                    row[ 3 ],  // region
                    row[ 4 ],  // quantity
                    row[ 6 ]   // total_amount
            );
        }

        if ( rows.size() > 5 )
        {
            System.out.println( "   ... and " + ( rows.size() - 5 ) + " more rows." );
        }
    }

    // ─── Private: Validation ──────────────────────────────────────────────────

    /**
     * Validates the file path before attempting to open it.
     * Throws specific, descriptive exceptions for each failure case.
     * <p>
     * Checks (in order — fail fast on first failure):
     * 1. Path is not null or blank
     * 2. File has .csv extension
     * 3. File exists on disk
     * 4. File is a regular file (not a directory)
     * 5. File is readable by the current process
     * 6. File is not empty (zero bytes = nothing to read)
     *
     * @throws IllegalArgumentException if path is null/blank or wrong extension
     * @throws FileNotFoundException    if file does not exist
     * @throws IllegalArgumentException if path is a directory, not readable, or empty
     */
    private static void validateFilePath( String filePath )
            throws FileNotFoundException
    {

        // Check 1: null or blank path
        if ( filePath == null || filePath.trim().isEmpty() )
        {
            throw new IllegalArgumentException(
                    "File path cannot be null or blank." );
        }

        // Check 2: must be a .csv file
        if ( ! filePath.toLowerCase().endsWith( CSV_EXTENSION ) )
        {
            throw new IllegalArgumentException(
                    "File must have a .csv extension. Received: \"" +
                            filePath + "\". This tool only processes CSV files." );
        }

        File file = new File( filePath );

        // Check 3: file must exist
        if ( ! file.exists() )
        {
            throw new FileNotFoundException(
                    "CSV file not found at path: \"" + filePath + "\". " +
                            "Check the path and try again." );
        }

        // Check 4: must be a file, not a directory
        if ( ! file.isFile() )
        {
            throw new IllegalArgumentException(
                    "Path points to a directory, not a file: \"" + filePath + "\"." );
        }

        // Check 5: must be readable by current process
        if ( ! file.canRead() )
        {
            throw new IllegalArgumentException(
                    "No read permission for file: \"" + filePath + "\". " +
                            "Check file permissions." );
        }

        // Check 6: file must not be empty
        if ( file.length() == 0 )
        {
            throw new IllegalArgumentException(
                    "CSV file is empty (0 bytes): \"" + filePath + "\". " +
                            "Nothing to read." );
        }
    }

    /**
     * Validates a single data row before applying the mapping.
     * <p>
     * Returns a skip reason string if the row is invalid,
     * or null if the row is valid (null = pass, proceed with this row).
     * <p>
     * Checks:
     * 1. Row must have at least 1 column after split
     * 2. Row must not be entirely whitespace values
     * <p>
     * Note: We do NOT check column count against STANDARD_COLUMNS.length here —
     * that's handled inside applyMapping() which fills missing columns with "".
     * This is intentional: a row with fewer columns than the header is malformed
     * but recoverable — we fill what we can and log it.
     *
     * @param cols       Split columns from one CSV line
     * @param mapping    Current column mapping
     * @param lineNumber Line number for error messages
     * @return Skip reason string, or null if valid
     */
    private static String validateDataRow( String[] cols,
                                           int[] mapping,
                                           int lineNumber )
    {
        // Check 1: must have at least 1 column
        if ( cols == null || cols.length == 0 )
        {
            return "row has no columns after split";
        }

        // Check 2: row must not be all-whitespace values
        boolean allBlank = true;
        for ( String col : cols )
        {
            if ( col != null && ! col.trim().isEmpty() )
            {
                allBlank = false;
                break;
            }
        }
        if ( allBlank )
        {
            return "all columns are blank";
        }

        return null; // valid row — proceed
    }

    // ─── Private: Audit & Display ─────────────────────────────────────────────

    /**
     * Prints the post-read audit summary.
     * Shows valid row count, skipped count, and details of every skipped row.
     */
    private static void printAuditSummary( int validCount, List<String> skippedRows )
    {
        System.out.println( "\n" + "─".repeat( 60 ) );
        System.out.println( "  📊 INGESTION AUDIT SUMMARY" );
        System.out.println( "─".repeat( 60 ) );
        System.out.printf( "  ✅ Valid rows loaded  : %d%n", validCount );
        System.out.printf( "  ⚠️  Rows skipped       : %d%n", skippedRows.size() );

        if ( ! skippedRows.isEmpty() )
        {
            System.out.println( "\n  Skipped row details:" );
            for ( String skipped : skippedRows )
            {
                System.out.println( "    → " + skipped );
            }
        }
        System.out.println( "─".repeat( 60 ) + "\n" );
    }

    /**
     * Builds a consistent, structured skip message for the audit log.
     * Format: "Line N | Reason: <reason> | Data: <first 80 chars>"
     */
    private static String buildSkipMessage( int lineNumber, String line, String reason )
    {
        String preview = ( line.length() > 80 )
                ? line.substring( 0, 80 ) + "..."
                : line;
        return String.format( "Line %-4d | Reason: %-45s | Data: %s",
                lineNumber, reason, preview );
    }
}




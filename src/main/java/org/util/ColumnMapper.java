package org.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.io.*;
import java.util.*;
import java.util.logging.*;


public class ColumnMapper
{

    /**
     * ColumnMapper — Production-grade column mapping engine.
     * <p>
     * Responsibilities:
     * 1. Read ANY CSV's headers
     * 2. Interactively map them to our standard internal schema
     * 3. Persist mapping to disk for reuse
     * 4. Load and validate saved mappings on subsequent runs
     * <p>
     * Design principles applied:
     * - Single Responsibility: mapping logic only, no CSV reading
     * - Fail-fast with meaningful messages on every error
     * - No magic numbers — all constants named
     * - Full try-catch on every I/O and parse operation
     * - Input retry loops — never crashes on bad user input
     * - Logging instead of raw System.err for auditability
     */

    // ─── Constants ─────────────────────────────────────────────────────────────

    private static final Logger LOGGER =
            Logger.getLogger( ColumnMapper.class.getName() );

    private static final String MAPPING_FILE =
            "src/main/resources/column_mapping.properties";

    private static final int MAX_INPUT_RETRIES = 3;

    /**
     * Standard schema this application always works with internally.
     * ALL other modules (DBLoader, QueryEngine) depend on this order.
     * Never reorder without updating those modules.
     * <p>
     * Index → column name:
     * 0 = order_date
     * 1 = product_name
     * 2 = category
     * 3 = region
     * 4 = quantity
     * 5 = unit_price
     * 6 = total_amount
     * 7 = customer_id
     */
    public static final String[] STANDARD_COLUMNS = {
            "order_date",
            "product_name",
            "category",
            "region",
            "quantity",
            "unit_price",
            "total_amount",
            "customer_id"
    };

    /**
     * Human-readable labels shown during the mapping wizard.
     * Must stay in sync with STANDARD_COLUMNS (same length, same order).
     */
    private static final String[] DISPLAY_NAMES = {
            "Order Date     (e.g. 2024-01-15, YYYY-MM-DD format)",
            "Product Name   (e.g. Laptop Pro)",
            "Category       (e.g. Electronics, Furniture)",
            "Region         (e.g. North, South, East, West)",
            "Quantity       (e.g. 5  — whole number)",
            "Unit Price     (e.g. 75000.00 — decimal)",
            "Total Amount   (e.g. 150000.00 — decimal)",
            "Customer ID    (e.g. CUST001)"
    };

    /**
     * Columns that MUST be mapped (cannot be -1).
     * These are used in SQL analytics — missing them breaks queries.
     * 0 = order_date   (used in date/monthly queries)
     * 1 = product_name (used in GROUP BY)
     * 4 = quantity     (used in aggregations)
     * 6 = total_amount (used in SUM, AVG)
     */
    private static final Set<Integer> REQUIRED_COLUMN_INDICES =
            Collections.unmodifiableSet( new HashSet<>( Arrays.asList( 0, 1, 4, 6 ) ) );

    // ─── Public API ────────────────────────────────────────────────────────────

    /**
     * Main entry point for the mapping flow.
     * <p>
     * Algorithm:
     * 1. Validate csvHeaders input
     * 2. Try to load a saved mapping from disk
     * 3. If found → show it → ask user to reuse or remap
     * 4. If not found or user says no → run interactive wizard
     * 5. Save the new mapping to disk
     *
     * @param csvHeaders Header row split from the CSV file (must not be null/empty)
     * @return int[] of length STANDARD_COLUMNS.length
     * result[i] = CSV column index for STANDARD_COLUMNS[i]
     * result[i] = -1 means that column is absent in this CSV
     * @throws IllegalArgumentException if csvHeaders is null or empty
     * @throws IOException              if mapping file cannot be read/written
     */
    public static int[] getMapping( String[] csvHeaders ) throws IOException
    {

        // ── Guard: csvHeaders must be valid ────────────────────────────────────
        if ( csvHeaders == null || csvHeaders.length == 0 )
        {
            throw new IllegalArgumentException(
                    "CSV headers cannot be null or empty. Ensure the CSV file has a valid header row." );
        }

        // Trim all headers defensively — never trust raw user file input
        String[] trimmedHeaders = trimAll( csvHeaders );

        // ── Try loading saved mapping ──────────────────────────────────────────
        int[] savedMapping = loadSavedMapping( trimmedHeaders.length );

        if ( savedMapping != null )
        {
            System.out.println( "\n💾 Found a saved column mapping from a previous session." );
            printMappingSummary( savedMapping, trimmedHeaders );

            boolean reuse = askYesNo( "   Reuse this saved mapping?" );
            if ( reuse )
            {
                System.out.println( "✅ Using saved mapping.\n" );
                return savedMapping;
            }
            System.out.println( "🔄 Starting fresh mapping wizard...\n" );
        }

        // ── Run interactive mapping wizard ─────────────────────────────────────
        int[] mapping = runMappingWizard( trimmedHeaders );

        // ── Persist mapping ────────────────────────────────────────────────────
        saveMapping( mapping );
        System.out.println( "💾 Mapping saved to: " + MAPPING_FILE );
        System.out.println( "   (Won't ask again unless you choose to remap)\n" );

        return mapping;
    }

    /**
     * Applies a column mapping to one raw CSV data row.
     * <p>
     * Converts the CSV's arbitrary column order into our fixed
     * STANDARD_COLUMNS order. Missing columns (-1) become empty
     * string — never null (null would cause NullPointerExceptions
     * downstream in PreparedStatement).
     *
     * @param rawCols Raw split columns from one CSV data line (must not be null)
     * @param mapping The int[] mapping returned by getMapping() (must not be null)
     * @return String[] of exactly STANDARD_COLUMNS.length values, never null elements
     * @throws IllegalArgumentException if rawCols or mapping is null
     */
    public static String[] applyMapping( String[] rawCols, int[] mapping )
    {
        if ( rawCols == null )
        {
            throw new IllegalArgumentException(
                    "rawCols cannot be null in applyMapping()" );
        }
        if ( mapping == null )
        {
            throw new IllegalArgumentException(
                    "mapping cannot be null in applyMapping()" );
        }

        String[] standardRow = new String[ STANDARD_COLUMNS.length ];

        for ( int i = 0 ; i < STANDARD_COLUMNS.length ; i++ )
        {
            int csvIndex = mapping[ i ];

            if ( csvIndex == - 1 )
            {
                // Column intentionally absent — use empty string, NEVER null
                standardRow[ i ] = "";

            }
            else if ( csvIndex >= 0 && csvIndex < rawCols.length )
            {
                // Valid index — trim the value and guard against null elements
                String value = rawCols[ csvIndex ];
                standardRow[ i ] = ( value != null ) ? value.trim() : "";

            }
            else
            {
                // Index out of bounds — this row has fewer columns than the header
                // (malformed data row). Log it and use empty string.
                LOGGER.warning( String.format(
                        "Column index %d out of bounds for row with %d columns. " +
                                "Standard column: %s. Using empty string.",
                        csvIndex, rawCols.length, STANDARD_COLUMNS[ i ] ) );
                standardRow[ i ] = "";
            }
        }

        return standardRow;
    }

    /**
     * Deletes the saved mapping file, forcing re-mapping on next run.
     * Use when the user switches to a CSV from a different source.
     *
     * @return true if deleted, false if file did not exist
     */
    public static boolean clearSavedMapping()
    {
        File file = new File( MAPPING_FILE );

        if ( ! file.exists() )
        {
            System.out.println( "ℹ️  No saved mapping found to clear." );
            return false;
        }

        boolean deleted = file.delete();
        if ( deleted )
        {
            System.out.println( "🗑️  Saved mapping cleared. " +
                    "Mapping wizard will run on next CSV load." );
        }
        else
        {
            LOGGER.warning( "Failed to delete mapping file: " +
                    file.getAbsolutePath() );
            System.out.println( "⚠️  Could not clear mapping file. " +
                    "Check file permissions at: " + MAPPING_FILE );
        }
        return deleted;
    }

    // ─── Private: Mapping Wizard ───────────────────────────────────────────────

    /**
     * Runs the full interactive mapping wizard.
     * <p>
     * For each standard column:
     * - Shows the user all available CSV columns with their index numbers
     * - Asks which index maps to this standard column
     * - Validates input with retry logic
     * - Enforces required columns cannot be skipped (-1)
     * <p>
     * At the end, shows a full summary and asks for confirmation.
     * If user says no → restarts recursively.
     *
     * @param csvHeaders Trimmed CSV header names
     * @return Validated int[] mapping
     * @throws IOException if user exceeds retry limits on any field
     */
    private static int[] runMappingWizard( String[] csvHeaders ) throws IOException
    {
        Scanner sc = new Scanner( System.in );
        int[] mapping = new int[ STANDARD_COLUMNS.length ];

        printDivider( "🔗 COLUMN MAPPING WIZARD" );

        System.out.println( "Your CSV has " + csvHeaders.length + " columns:\n" );
        for ( int i = 0 ; i < csvHeaders.length ; i++ )
        {
            System.out.printf( "   [%d] %s%n", i, csvHeaders[ i ] );
        }

        System.out.println();
        System.out.println( "For each field below, enter the NUMBER of your matching column." );
        System.out.println( "Enter -1 if that field does not exist in your CSV." );
        System.out.println( "Fields marked * are required and cannot be skipped.\n" );

        for ( int i = 0 ; i < STANDARD_COLUMNS.length ; i++ )
        {
            boolean isRequired = REQUIRED_COLUMN_INDICES.contains( i );
            String label = ( isRequired ? "* " : "  " ) + DISPLAY_NAMES[ i ];

            mapping[ i ] = askColumnIndex( sc, label, csvHeaders.length, isRequired );

            // Echo back what was chosen for immediate feedback
            if ( mapping[ i ] == - 1 )
            {
                System.out.println( "      ↳ ⚠️  Not mapped (will store as empty string)\n" );
            }
            else
            {
                System.out.printf( "      ↳ ✅ Mapped to column [%d]: \"%s\"%n%n",
                        mapping[ i ], csvHeaders[ mapping[ i ] ] );
            }
        }

        // Final summary + confirmation before saving
        printDivider( "📋 MAPPING CONFIRMATION" );
        printMappingSummary( mapping, csvHeaders );

        boolean confirmed = askYesNo( "\n   Confirm and save this mapping?" );

        if ( ! confirmed )
        {
            System.out.println( "\n🔄 Restarting mapping wizard...\n" );
            return runMappingWizard( csvHeaders );  // let user redo it completely
        }

        return mapping;
    }

    /**
     * Asks user to enter a column index with full validation and retry logic.
     * <p>
     * Validation rules (all enforced before accepting):
     * 1. Input must not be blank
     * 2. Input must parse as a valid integer
     * 3. If required: input cannot be -1
     * 4. If not -1: must be in range 0..(maxIndex-1)
     * <p>
     * After MAX_INPUT_RETRIES consecutive failures → throws IOException.
     * This is intentional: we do NOT loop forever. A completely
     * unresponsive or broken input should fail loudly, not hang silently.
     *
     * @param sc         Scanner for reading System.in
     * @param label      Display label shown to user
     * @param maxIndex   Number of columns in CSV (valid range: 0..maxIndex-1)
     * @param isRequired If true, -1 is rejected
     * @return Validated column index
     * @throws IOException if valid input not received within MAX_INPUT_RETRIES
     */
    private static int askColumnIndex( Scanner sc, String label,
                                       int maxIndex, boolean isRequired )
            throws IOException
    {

        int attempts = 0;

        while ( attempts < MAX_INPUT_RETRIES )
        {
            System.out.printf( "   %-58s → ", label );

            String rawInput;
            try
            {
                rawInput = sc.nextLine();
            }
            catch ( NoSuchElementException | IllegalStateException e )
            {
                // Scanner closed or System.in ended — fatal, cannot recover
                throw new IOException(
                        "Input stream closed unexpectedly during column mapping. " +
                                "Cannot continue.", e );
            }

            // ── Rule 1: Blank input ────────────────────────────────────────────
            if ( rawInput == null || rawInput.trim().isEmpty() )
            {
                attempts++;
                System.out.println( "      ❌ Input cannot be blank. " +
                        remainingAttempts( attempts ) );
                continue;
            }

            // ── Rule 2: Must be a valid integer ───────────────────────────────
            int value;
            try
            {
                value = Integer.parseInt( rawInput.trim() );
            }
            catch ( NumberFormatException e )
            {
                attempts++;
                System.out.printf( "      ❌ \"%s\" is not a valid number. " +
                                "Enter 0-%d, or -1 to skip. %s%n",
                        rawInput.trim(), maxIndex - 1,
                        remainingAttempts( attempts ) );
                continue;
            }

            // ── Rule 3: Required fields cannot be -1 ─────────────────────────
            if ( value == - 1 && isRequired )
            {
                attempts++;
                System.out.println( "      ❌ This field is required (*) and cannot be skipped. " +
                        remainingAttempts( attempts ) );
                continue;
            }

            // ── Rule 4: Must be -1 or within valid column range ───────────────
            if ( value != - 1 && ( value < 0 || value >= maxIndex ) )
            {
                attempts++;
                System.out.printf( "      ❌ %d is out of range. " +
                                "Valid range is 0 to %d. %s%n",
                        value, maxIndex - 1,
                        remainingAttempts( attempts ) );
                continue;
            }

            // All rules passed — return the valid value
            return value;
        }

        // Exceeded retry limit — throw IOException so caller can handle it
        // cleanly instead of proceeding with a corrupt/incomplete mapping
        throw new IOException( String.format(
                "Failed to get valid input for field [%s] after %d attempts. " +
                        "Aborting mapping to prevent data corruption.",
                label.trim(), MAX_INPUT_RETRIES ) );
    }

    /**
     * Asks a yes/no question. Retries indefinitely until valid answer given.
     * Accepts: yes, y, no, n (case-insensitive).
     * On Scanner failure, defaults to false (safe default — don't proceed blindly).
     *
     * @param question Question text (without the yes/no prompt)
     * @return true = yes, false = no
     */
    private static boolean askYesNo( String question )
    {
        Scanner sc = new Scanner( System.in );

        while ( true )
        {
            System.out.print( question + " (yes/no): " );

            String input;
            try
            {
                input = sc.nextLine();
            }
            catch ( NoSuchElementException | IllegalStateException e )
            {
                LOGGER.warning( "Input stream closed during yes/no prompt. " +
                        "Defaulting to 'no' (safe default)." );
                return false;
            }

            if ( input == null || input.trim().isEmpty() )
            {
                System.out.println( "   ❌ Please type 'yes' or 'no'." );
                continue;
            }

            String normalized = input.trim().toLowerCase();

            if ( normalized.equals( "yes" ) || normalized.equals( "y" ) )
            {
                return true;
            }
            if ( normalized.equals( "no" ) || normalized.equals( "n" ) )
            {
                return false;
            }

            System.out.printf( "   ❌ \"%s\" is not valid. Please type 'yes' or 'no'.%n",
                    input.trim() );
        }
    }

    // ─── Private: Persistence ──────────────────────────────────────────────────

    /**
     * Saves the mapping to a .properties file for future reuse.
     * <p>
     * File format example:
     * order_date_col=0
     * product_name_col=1
     * category_col=2
     * ...
     * <p>
     * Creates parent directories if they don't exist.
     * Uses try-with-resources to guarantee stream closure even on exception.
     *
     * @param mapping Validated int[] mapping to persist
     * @throws IOException if directories cannot be created or file cannot be written
     */
    private static void saveMapping( int[] mapping ) throws IOException
    {
        Properties props = new Properties();

        for ( int i = 0 ; i < STANDARD_COLUMNS.length ; i++ )
        {
            props.setProperty(
                    STANDARD_COLUMNS[ i ] + "_col",
                    String.valueOf( mapping[ i ] ) );
        }

        File file = new File( MAPPING_FILE );
        File parentDir = file.getParentFile();

        // Create parent directories if they don't exist
        if ( parentDir != null && ! parentDir.exists() )
        {
            boolean created = parentDir.mkdirs();
            if ( ! created )
            {
                throw new IOException(
                        "Could not create directory for mapping file: " +
                                parentDir.getAbsolutePath() +
                                ". Check directory permissions." );
            }
        }

        // try-with-resources guarantees stream is closed even if write fails
        try ( FileOutputStream fos = new FileOutputStream( file ) )
        {
            props.store( fos,
                    "SmartSalesAnalyser - Column Mapping (auto-generated)\n" +
                            "# Modify with caution. Delete this file to trigger re-mapping." );
        }
        catch ( IOException e )
        {
            // Wrap with context so caller knows which file failed
            throw new IOException(
                    "Failed to write mapping to file: " + MAPPING_FILE +
                            ". Cause: " + e.getMessage(), e );
        }
    }

    /**
     * Loads and validates a previously saved mapping.
     * <p>
     * Validation:
     * - All STANDARD_COLUMNS must have a property key present
     * - All values must parse as integers
     * - All values must be -1 or within 0..(maxCsvColumns-1)
     * <p>
     * Returns null (not exception) if file doesn't exist or is invalid.
     * Returning null is the correct signal to run the wizard again —
     * it is NOT an error, it is expected on first run.
     *
     * @param maxCsvColumns Number of columns in current CSV for range validation
     * @return Validated int[] mapping, or null if unavailable/invalid
     */
    private static int[] loadSavedMapping( int maxCsvColumns )
    {
        File file = new File( MAPPING_FILE );

        if ( ! file.exists() )
        {
            LOGGER.info( "No saved mapping at: " + MAPPING_FILE + ". Will run wizard." );
            return null;
        }

        Properties props = new Properties();

        // try-with-resources — stream always closed even if load() throws
        try ( FileInputStream fis = new FileInputStream( file ) )
        {
            props.load( fis );
        }
        catch ( IOException e )
        {
            LOGGER.warning( "Could not read mapping file: " + e.getMessage() +
                    ". Will run wizard." );
            return null;
        }

        int[] mapping = new int[ STANDARD_COLUMNS.length ];

        for ( int i = 0 ; i < STANDARD_COLUMNS.length ; i++ )
        {
            String key = STANDARD_COLUMNS[ i ] + "_col";
            String value = props.getProperty( key );

            // Missing key — file is incomplete, discard it entirely
            if ( value == null )
            {
                LOGGER.warning( "Saved mapping missing key: " + key +
                        ". Discarding saved mapping." );
                return null;
            }

            // Cannot parse as integer — file is corrupt
            int colIndex;
            try
            {
                colIndex = Integer.parseInt( value.trim() );
            }
            catch ( NumberFormatException e )
            {
                LOGGER.warning( "Corrupt value in saved mapping. Key: " + key +
                        ", Value: \"" + value + "\". Discarding saved mapping." );
                return null;
            }

            // Index out of range for the current CSV
            // (user may have loaded a CSV with fewer columns than when they last mapped)
            if ( colIndex != - 1 && ( colIndex < 0 || colIndex >= maxCsvColumns ) )
            {
                LOGGER.warning( "Saved mapping index " + colIndex + " for key \"" + key +
                        "\" is out of range for CSV with " + maxCsvColumns + " columns." );
                System.out.println( "\n⚠️  Saved mapping is incompatible with this CSV " +
                        "(CSV column count has changed). Mapping wizard will run." );
                return null;
            }

            mapping[ i ] = colIndex;
        }

        return mapping;
    }

    // ─── Private: Display Helpers ──────────────────────────────────────────────

    /**
     * Prints a formatted table showing every standard column and what CSV column it maps to.
     */
    private static void printMappingSummary( int[] mapping, String[] csvHeaders )
    {
        System.out.println();
        System.out.printf( "   %-22s %-6s  %s%n",
                "STANDARD COLUMN", "INDEX", "YOUR CSV COLUMN" );
        System.out.println( "   " + "─".repeat( 58 ) );

        for ( int i = 0 ; i < STANDARD_COLUMNS.length ; i++ )
        {
            int csvIndex = mapping[ i ];
            boolean required = REQUIRED_COLUMN_INDICES.contains( i );

            String idxStr;
            String csvColName;

            if ( csvIndex == - 1 )
            {
                idxStr = " -1 ";
                csvColName = "⚠️  Not mapped (optional)";
            }
            else if ( csvIndex < csvHeaders.length )
            {
                idxStr = " [" + csvIndex + "] ";
                csvColName = csvHeaders[ csvIndex ];
            }
            else
            {
                idxStr = " [" + csvIndex + "] ";
                csvColName = "❌ INVALID (index out of range)";
            }

            System.out.printf( "   %-22s %-6s  %s%s%n",
                    STANDARD_COLUMNS[ i ],
                    idxStr,
                    csvColName,
                    required ? "  ← required" : "" );
        }
    }

    private static void printDivider( String title )
    {
        System.out.println( "\n" + "─".repeat( 65 ) );
        System.out.println( "  " + title );
        System.out.println( "─".repeat( 65 ) );
    }

    private static String remainingAttempts( int attempts )
    {
        int remaining = MAX_INPUT_RETRIES - attempts;
        if ( remaining <= 0 )
        {
            return "(last attempt failed — will abort)";
        }
        return "(" + remaining + " attempt" + ( remaining == 1 ? "" : "s" ) + " remaining)";
    }

    /**
     * Returns a new trimmed copy of every element in arr.
     * Null elements become empty string — never propagate nulls.
     */
    private static String[] trimAll( String[] arr )
    {
        String[] result = new String[ arr.length ];

        for ( int i = 0 ; i < arr.length ; i++ )
        {
            result[ i ] = ( arr[ i ] != null ) ? arr[ i ].trim() : "";
        }

        return result;
    }
}




/**
 * Copyright (C) 1998-2011 enStratusNetworks LLC
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

/* $Id: DaseinSequencer.java,v 1.4 2009/02/07 18:11:51 greese Exp $ */
/* Copyright (c) 2002-2004 Valtira Corporation, All Rights Reserved */
package org.dasein.persist;

// Developed by George Reese for the book:
// Java Best Practices, Volume II: J2EE
// Ported to the digital@jwt code library by George Reese

// J2SE imports
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

/**
 * <p>
 * Provides a database-independent sequence generation scheme. This class
 * goes to the database once every <code>MAX_KEYS</code> requests to
 * get a new seed for the numbers it generates. This class is thread-safe,
 * meaning multiple threads can be safely requesting unique numbers from it.
 * It is also multi-process safe. In other words, multiple machines can
 * simultaneously be generating unique values and those values will
 * be guaranteed to be unique across all applications. The only caveat
 * is that they all must be using the same algorithm for generating
 * the numbers and getting seeds from the same database.
 * </p>
 * <p>
 * In order to access the database, this class reads the configuration
 * properties file <i>dasein-persistence.properties</i> to look up the DSN
 * for the sequencer database. This value should be set with the
 * name of the DSN that provides connections to the database with the
 * <code>sequencer</code> table. That table should have the
 * following <code>CREATE</code>:
 * </p>
 * <pre>
 * CREATE TABLE sequencer (
 *     name        VARCHAR(20)     NOT NULL,
 *     spacing    BIGINT UNSIGNED NOT NULL,
 *     next_key     BIGINT UNSIGNED NOT NULL,
 *     last_update  BIGINT UNSIGNED NOT NULL,
 *     PRIMARY KEY ( name, lastUpdate )
 * );
 * </pre>
 * <p>
 * Last modified $Date: 2009/02/07 18:11:51 $
 * </p>
 * @version $Revision: 1.4 $
 * @author George Reese
 */
public class DaseinSequencer extends Sequencer {
    static private final Logger logger = Logger.getLogger(DaseinSequencer.class);
    
    static private final long   defaultInterval;

    static private final String  dataSourceName;
    
    static {
        Properties props = new Properties();
        String dsn, istr;
        long di;
        
        try {
            InputStream is = DaseinSequencer.class.getResourceAsStream(PROPERTIES);

            if( is != null ) {
                props.load(is);
            }
        }
        catch( Exception e ) {
            logger.error("Problem loading " + PROPERTIES + ": " + e.getMessage(), e);
        }
        dsn = props.getProperty("dasein.seqdsn");
        dataSourceName = dsn;
        istr = props.getProperty("dasein.seqint");
        if( istr == null ) {
            di = 100L; 
        }
        else {
            try {
                di = Long.parseLong(istr);
            }
            catch( NumberFormatException e ) {
                di = 100L;
            }
        }
        defaultInterval = di;
    }
    
    /**
     * The interval governing this sequence.
     */
    private long interval = defaultInterval;
    /**
     * The current sequence within this sequencer's seed.
     */
    private long sequence = 0L;
    /**
     * The next sequence in the database, need to regen when encountered.
     */
    private long nextKey  = 1L;

    public DaseinSequencer() {
        super();
    }
    
   /**
     * The SQL for creating a new sequence in the database.
     */
    static private final String CREATE_SEQ =
        "INSERT INTO sequencer ( name, next_key, spacing, last_update ) " +
        "VALUES ( ?, ?, ?, ? )";
    
    /**
     * Constant for the name parameter.
     */
    static private final int INS_NAME     = 1;
    /**
     * Constant for the seed parameter.
     */
    static private final int INS_NEXT_KEY = 2;
    /**
     * Constant for the interval parameter.
     */
    static private final int INS_INTERVAL = 3;
    /**
     * Constant for the lastUpdate parameter
     */
    static private final int INS_UPDATE   = 4;

    /**
     * Creates a new entry in the database for this sequence. This method
     * will throw an error if two threads are simultaneously trying
     * to create a sequence. This state should never occur if you
     * go ahead and create the sequence in the database before
     * deploying the application. It could be avoided by checking
     * SQL exceptions for the proper XOPEN SQLState for duplicate
     * keys. Unfortunately, that approach is error prone due to the lack
     * of consistency in proper XOPEN SQLState reporting in JDBC drivers.
     * @param conn the JDBC connection to use
     * @throws java.sql.SQLException a database error occurred
     */
    private void create(Connection conn) throws SQLException {
        logger.debug("enter - create()");
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            
            try {
                stmt = conn.prepareStatement(CREATE_SEQ);
                stmt.setString(INS_NAME, getName());
                stmt.setLong(INS_NEXT_KEY, nextKey);
                stmt.setLong(INS_INTERVAL, interval);
                stmt.setLong(INS_UPDATE, System.currentTimeMillis());
                if( stmt.executeUpdate() != 1 ) {
                    logger.warn("Unable to create sequence " + getName() + ".");
                    sequence = -1L;
                }
            }
            finally {
                if( rs != null ) {
                    try { rs.close(); }
                    catch( SQLException ignore ) { /* ignore */}
                }
                if( stmt != null ) {
                    try { stmt.close(); }
                    catch( SQLException ignore ) { /* ignore */ }
                }
            }
        }
        finally {
            logger.debug("exit - create()");
        }
    }

    /**
     * Generates a new unique number. The unique number is based on the
     * following algorithm:<br/>
     * <i>unique number</i> = <i>seed</i> multiple by
     * <i>maximum keys per seed</i> added to <i>seed sequence</i>
     * <br/>
     * The method then increments the seed sequence for the next
     * ID to be generated. If the ID to be generated would exhaust
     * the seed, then a new seed is retrieved from the database.
     * @return a unique number
     * @throws org.dasein.persist.PersistenceException a data store error
     * occurred while generating the number
     */
    public synchronized long next() throws PersistenceException {
        logger.debug("enter - next()");
        try {
            Connection conn = null;
            
            sequence++;
            if( logger.isInfoEnabled() ) {
                logger.info("Getting next ID for " + getName() +
                            " (" + sequence + ").");
            }
            if( sequence == nextKey ) {
                logger.info("Key space exhausted for " + getName() + ".");
                try {
                    InitialContext ctx = new InitialContext();
                    DataSource ds = (DataSource)ctx.lookup(dataSourceName);
                    conn = ds.getConnection();
                    conn.setReadOnly(false); // force read-only to be reset to false
                    reseed(conn);
                    if( !conn.getAutoCommit() ) {
                        conn.commit();
                    }
                }
                catch( SQLException e ) {
                    throw new PersistenceException(e);
                }
                catch( NamingException e ) {
                    throw new PersistenceException(e);
                }
                finally {
                    if( conn != null ) {
                        try { conn.close(); }
                        catch( SQLException ignore ) { /* ignore */ }
                    }
                }
            }
            logger.info("Returning sequence " + sequence + " for " +
                        getName() + ".");
            // the next key for this sequencer
            return sequence;
        }
        finally {
            logger.debug("exit - next()");
        }
    }

    /**
     * The SQL for getting a seed for a sequence from the database.
     */
    static private final String FIND_SEQ =
        "SELECT next_key, spacing, last_update " +
        "FROM sequencer " +
        "WHERE name = ?";
    /**
     * Constant for the name parameter.
     */
    static private final int SEL_NAME   = 1;
    /**
     * Constant for the next key column.
     */
    static private final int SEL_NEXT_KEY   = 1;
    /**
     * Constant for the interval column.
     */
    static private final int SEL_INTERVAL   = 2;
    /**
     * Constant for the lastUpdate column.
     */
    static private final int SEL_UPDATE     = 3;
    /**
     * The SQL for incrementing the seed in the database.
     */
    static private String UPDATE_SEQ =
        "UPDATE sequencer " +
        "SET next_key = ?, " +
        "last_update = ? " +
        "WHERE name = ? AND next_key = ? AND last_update = ?";
    /**
     * Constant for the seed parameter.
     */
    static private final int UPD_NEXT_KEY     = 1;
    /**
     * Constant for the lastUpdate set parameter
     */
    static private final int UPD_SET_UPDATE   = 2;
    /**
     * Constant for the name parameter.
     */
    static private final int UPD_NAME         = 3;
    /**
     * Constant for the next key where parameter.
     */
    static private final int UPD_WHERE_KEY    = 4;
    /**
     * Constant for the lastUpdate parameter.
     */
    static private final int UPD_WHERE_UPDATE = 5;

    /**
     * Gets the next seed from the database for this sequence.
     * @param conn the database connection
     * @throws java.sql.SQLException a database error occurred
     */
    private void reseed(Connection conn) throws SQLException {
        logger.debug("enter - reseed()");
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            
            try {
                // Keep in this loop as long as we encounter concurrency errors
                do {
                    stmt = conn.prepareStatement(FIND_SEQ);
                    stmt.setString(SEL_NAME, getName());
                    rs = stmt.executeQuery();
                    if( !rs.next() ) {
                        logger.info("No sequence in DB for " + getName() + ".");
                        // no such sequence, create it
                        {
                            // close resources
                            try { rs.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            rs = null;
                            try { stmt.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            stmt = null;
                        }
                        sequence = 100L;
                        nextKey = sequence + interval;
                        create(conn);
                    }
                    else {
                        long ts;
                        
                        sequence = rs.getLong(SEL_NEXT_KEY);
                        interval = rs.getLong(SEL_INTERVAL);
                        if( interval < 1 ) {
                            interval = defaultInterval;
                        }
                        nextKey = sequence + interval;
                        ts = rs.getLong(SEL_UPDATE);
                        {
                            // close resources
                            try { rs.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            rs = null;
                            try { stmt.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            stmt = null;
                        }
                        // increment the seed in the database
                        stmt = conn.prepareStatement(UPDATE_SEQ);
                        stmt.setLong(UPD_NEXT_KEY, nextKey);
                        stmt.setLong(UPD_SET_UPDATE, System.currentTimeMillis());
                        stmt.setString(UPD_NAME, getName());
                        stmt.setLong(UPD_WHERE_KEY, sequence);
                        stmt.setLong(UPD_WHERE_UPDATE, ts);
                        if( stmt.executeUpdate() != 1 ) {
                            // someone changed the database! try again!
                            sequence = -1L;
                            logger.warn("Concurrency error, requerying DB.");
                        }
                        else {
                            if( !conn.getAutoCommit() ) {
                                conn.commit();
                            }
                        }
                    }
                } while( sequence == -1L );
                logger.info("Sequence set to " + sequence + ", next_key is " +
                            nextKey + ".");
            }
            finally {
                if( rs != null ) {
                    try { rs.close(); }
                    catch( SQLException ignore ) { /* ignore */ }
                }
                if( stmt != null ) {
                    try { stmt.close(); }
                    catch( SQLException ignore ) { /* ignore */ }
                }
            }   
        }
        finally {
            logger.debug("exit - reseed()");
        }
    }
}

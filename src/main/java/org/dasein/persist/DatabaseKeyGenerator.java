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

package org.dasein.persist;

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

public class DatabaseKeyGenerator extends Sequencer {
    static private final long    defaultInterval;
    static private final String  dataSourceName;
    
    static private final String FIND_SEQ =
        "SELECT next_key, spacing, last_update " +
        "FROM sequencer " +
        "WHERE name = ?";
    
    static private final int FIND_NAME   = 1;
    static private final int FIND_NEXT_KEY   = 1;
    static private final int FIND_INTERVAL   = 2;
    static private final int FIND_UPDATE     = 3;

    static private final String INSERT_SEQ =
        "INSERT INTO sequencer ( name, next_key, spacing, last_update ) " +
        "VALUES ( ?, ?, ?, ? )";
    
    static private final int INS_NAME     = 1;
    static private final int INS_NEXT_KEY = 2;
    static private final int INS_INTERVAL = 3;
    static private final int INS_UPDATE   = 4;
    
    static private String UPDATE_SEQ =
        "UPDATE sequencer " +
        "SET next_key = ?, " +
        "last_update = ? " +
        "WHERE name = ? AND next_key = ? AND last_update = ?";
    
    static private final int UPD_NEXT_KEY     = 1;
    static private final int UPD_SET_UPDATE   = 2;
    static private final int UPD_NAME         = 3;
    static private final int UPD_WHERE_KEY    = 4;
    static private final int UPD_WHERE_UPDATE = 5;
    
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
            Logger.getLogger(DatabaseKeyGenerator.class).error("Problem loading " + PROPERTIES + ": " + e.getMessage(), e);
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
    
    private long interval;
    private long nextId;
    private long nextSeed;
    
    public DatabaseKeyGenerator() { 
        nextId = 0L;
        interval = defaultInterval;
        nextSeed = nextId;
    }
    
    private transient volatile Logger logger = null;
    
    private Logger getLogger() {
        if( logger == null ) {
            logger = Logger.getLogger("org.dasein.sequencer." + getName());
        }
        return logger;
    }
    
    private void insert() throws SQLException, NamingException {
        Logger logger = getLogger();
        
        if( logger.isDebugEnabled() ) {
            logger.debug("enter - " + getClass().getName() + ".insert()");
        }
        try {
            if( logger.isInfoEnabled() ) {
                logger.info("insert(): Create new sequencer record for " + getName());
            }
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            
            try {
                InitialContext ctx = new InitialContext();
                DataSource ds = (DataSource)ctx.lookup(dataSourceName);
                
                conn = ds.getConnection();
                conn.setReadOnly(false);
                stmt = conn.prepareStatement(INSERT_SEQ);
                stmt.setString(INS_NAME, getName());
                stmt.setLong(INS_NEXT_KEY, defaultInterval*2);
                stmt.setLong(INS_INTERVAL, defaultInterval);
                stmt.setLong(INS_UPDATE, System.currentTimeMillis());
                try {
                    if( stmt.executeUpdate() != 1 ) {
                        nextId = -1L;
                        logger.warn("insert(): Failed to create sequencer entry for " + getName() + " (no error message)");
                    }
                    else if( logger.isInfoEnabled() ){
                        nextId = defaultInterval;
                        nextSeed = defaultInterval * 2;
                        interval = defaultInterval;
                        logger.info("insert(): First ID will be " + nextId);
                    }
                }
                catch( SQLException e ) {
                    String err = "insert(): Error inserting row into database, possible concurrency issue: " + e.getMessage();
                    if( logger.isDebugEnabled() ) {
                        logger.warn(err, e);
                    } else {
                        logger.warn(err);
                    }
                    nextId = -1L;
                }
                if( !conn.getAutoCommit() ) {
                    conn.commit();
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
                if( conn != null ) {
                    if( !conn.getAutoCommit() ) {
                        try { conn.rollback(); } 
                        catch( SQLException ignore ) { /* ignore */ }
                    }
                    try { conn.close(); }
                    catch( SQLException ignore ) { /* ignore */ }
                }
            }
        }
        finally {
            if( logger.isDebugEnabled() ) {
                logger.debug("exit - " + getClass().getName() + ".insert()");
            }
        }
    }
    
    @Override
    public synchronized long next() throws PersistenceException {
        Logger logger = getLogger();
        
        if( logger.isDebugEnabled() ) {
            logger.debug("enter - " + getClass().getName() + ".next()");
        }
        try {
            nextId++;
            if( logger.isInfoEnabled() ) {
                logger.info("next(): Next prospective ID is " + nextId);
            }
            if( nextId < 1L || nextId >= nextSeed ) {
                logger.info("next(): Key space exhausted for " + getName() + ".");
                try {
                    update();
                }
                catch( SQLException e ) {
                    String err = "next(): Failed to update key space: " + e.getMessage();
                    if( logger.isDebugEnabled() ) {
                        logger.error(err, e);
                    } else {
                        logger.error(err);
                    }
                    throw new PersistenceException(e);
                }
                catch( NamingException e ) {
                    String err = "next(): Failed to update key space: " + e.getMessage();
                    if( logger.isDebugEnabled() ) {
                        logger.error(err, e);
                    } else {
                        logger.error(err);
                    }
                    throw new PersistenceException(e);
                }
            }
            if( logger.isInfoEnabled() ) {
                logger.info("next(): " + nextId);
            }
            return nextId;
        }
        finally {
            logger.debug("exit - " + getClass().getName() + ".next()");
        }
    }
    
    private void update() throws SQLException, NamingException {
        Logger logger = getLogger();
        
        if( logger.isDebugEnabled() ) {
            logger.debug("enter - " + getClass().getName() + ".update()");
        }
        try {
            if( logger.isInfoEnabled() ) {
                logger.info("update(): Updating sequencer record for " + getName());
            }
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;
            
            try {
                InitialContext ctx = new InitialContext();
                DataSource ds = (DataSource)ctx.lookup(dataSourceName);
            
                // Keep in this loop as long as we encounter concurrency errors
                do {
                    conn = ds.getConnection();
                    conn.setReadOnly(false); // make sure read/write data source will be the same
                    stmt = conn.prepareStatement(FIND_SEQ);
                    stmt.setString(FIND_NAME, getName());
                    rs = stmt.executeQuery();
                    if( !rs.next() ) {
                        if( logger.isInfoEnabled() ) {
                            logger.info("update(): No sequence in DB for " + getName() + ".");
                        }
                        // no such sequence, create it
                        {
                            // close resources
                            try { rs.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            rs = null;
                            try { stmt.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            stmt = null;
                            try { conn.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            conn = null;
                        }
                        insert();
                    }
                    else {
                        long ts;
                        
                        nextId = rs.getLong(FIND_NEXT_KEY);
                        interval = rs.getLong(FIND_INTERVAL);
                        if( interval < 5 ) {
                            if( defaultInterval < 5 ) {
                                interval = 5;
                            }
                            else {
                                interval = defaultInterval;
                            }
                        }
                        nextSeed = nextId + interval;
                        ts = rs.getLong(FIND_UPDATE);
                        {
                            // close resources
                            try { rs.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            rs = null;
                            try { stmt.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            stmt = null;
                            try { conn.close(); }
                            catch( SQLException ignore ) { /* ignore */ }
                            conn = null;
                        }
                        // increment the seed in the database
                        conn = ds.getConnection();
                        conn.setReadOnly(false);
                        stmt = conn.prepareStatement(UPDATE_SEQ);
                        stmt.setLong(UPD_NEXT_KEY, nextSeed);
                        stmt.setLong(UPD_SET_UPDATE, System.currentTimeMillis());
                        stmt.setString(UPD_NAME, getName());
                        stmt.setLong(UPD_WHERE_KEY, nextId);
                        stmt.setLong(UPD_WHERE_UPDATE, ts);
                        if( stmt.executeUpdate() != 1 ) {
                            // someone changed the database! try again!
                            nextId = -1L;
                            logger.warn("update(): Concurrency error for " + getName() + ", requerying DB.");
                        }
                        if( !conn.getAutoCommit() ) {
                            conn.commit();
                        }
                    }
                } while( nextId == -1L );
                if( logger.isInfoEnabled() ) {
                    logger.info("update(): Next ID for " + getName() + " is set to " + nextId + " generating to " + nextSeed);
                }
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
                if( conn != null ) {
                    try { conn.close(); }
                    catch( SQLException ignore ) { /* ignore */ }
                }
            }   
        }
        finally {
            logger.debug("exit - " + getClass().getName() + ".update()");
        }
    }

}

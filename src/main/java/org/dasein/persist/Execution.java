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

/* $Id: Execution.java,v 1.17 2009/02/21 19:44:38 greese Exp $ */
/* Copyright (c) 2002-2004 Valtira Corporation, All Rights Reserved */
package org.dasein.persist;

// J2SE imports
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;

// J2EE imports
import javax.naming.NamingException;

// Apache imports
import org.apache.log4j.Logger;

import org.dasein.persist.dao.LoadTranslator;
import org.dasein.persist.dao.RemoveTranslator;
import org.dasein.persist.dao.SaveTranslator;
import org.dasein.util.DaseinUtilTasks;
import org.dasein.util.Translator;

/**
 * Represents a database event, generally a specific query or update.
 * You implement this class for all statements you wish to manage.
 * <br/>
 * Last modified: $Date: 2009/02/21 19:44:38 $
 * @version $Revision: 1.17 $
 * @author George Reese
 */
@SuppressWarnings("rawtypes")
public abstract class Execution {
    static public final String DASEIN_PERSIST_PROPERTIES = "daseinPersist";
    static public final String PROPERTIES = "/dasein-persistence.properties";
    
    static private final Logger logger = Logger.getLogger(Execution.class);

    /**
     * Cache of execution instances.
     */
    static private HashMap<String,Stack<Execution>> cache        = new HashMap<String,Stack<Execution>>();

    static private HashMap<String,String>           dataSources  = new HashMap<String,String>();
    
    /**
     * Loads the sequencers from the dasein-persistence.properties
     * configuration file.
     */
    static {
        try {
            
            InputStream is = Sequencer.class.getResourceAsStream(PROPERTIES);
            Properties props = new Properties();
            Enumeration propenum;

            logger.info("Looking up: " + PROPERTIES);
            logger.info("Location:   " + Sequencer.class.getResource(PROPERTIES));
            if( is != null ) {
                props.load(is);
            }
            else {
                // let's check the system environment properties now
                String path = System.getenv(DASEIN_PERSIST_PROPERTIES);
                if (path != null && !path.isEmpty()) {
                    is = new FileInputStream(path);
                    // todo: 'is' is ignored
                }
            }
            propenum = props.propertyNames();
            while( propenum.hasMoreElements() ) {
                String nom = (String)propenum.nextElement();

                if( nom.startsWith("dsn.") ) {
                    String dsn = props.getProperty(nom);
                    
                    nom = nom.substring(4);
                    dataSources.put(nom, dsn);
                }
            }
        }
        catch( Exception e ) {
            logger.error("Problem reading " + PROPERTIES + ": " + e.getMessage(), e);
        }
        DaseinUtilTasks.submit(new StackPusher());
    }

    private static class StackPusher implements Runnable {
        @Override
        public void run() {
            pushExecutions();
        }
    }

    static public String getDataSourceName(String cname) {
        return getDataSourceName(cname, false);
    }
    
    static public String getDataSourceName(String cname, boolean readOnly) {
        String postfix = readOnly ? ".read" : ".write";
        String nom = null;
        
        while( !dataSources.containsKey(cname + postfix) && !dataSources.containsKey(cname) ) {
            int idx = cname.lastIndexOf(".");
            
            if( idx != -1 ) {
                cname = cname.substring(0, idx);
            }
            else {
                break;
            }
        }
        if( dataSources.containsKey(cname + postfix) )  {
            nom = dataSources.get(cname + postfix);
        }
        else if( dataSources.containsKey(cname) ) {
            nom = dataSources.get(cname);
        }        
        return nom;
    }
    
    /**
     * Provides an instance of an execution object for a specific
     * subclass. Executions are stored in a hash map of stacks.
     * This enables an instance to be reused, if possible. Otherwise a
     * new one is created.
     * @param cls the subclass of <code>Execution</code> to be
     * retrieved
     * @return an instance of the specified class
     */
    @SuppressWarnings("unchecked")
    static public <T extends Execution> T getInstance(Class<T> cls) {
        logger.debug("enter - getInstance()");
        try {
            Stack<Execution> stack;
            
            synchronized( cache ) {
                if( !cache.containsKey(cls.getName()) ) {
                    return cls.newInstance();
                }
                else {
                    stack = cache.get(cls.getName());
                }
            }
            synchronized( stack ) {
                if( stack.empty() ) {
                    return cls.newInstance();
                }
                else {
                    return (T)stack.pop();
                }
            }
        }
        catch( InstantiationException e ) {
            throw new RuntimeException(e.getMessage());
        }
        catch( IllegalAccessException e ) {
            throw new RuntimeException(e.getMessage());
        }
        finally {
            logger.debug("exit - getInstance()");
        }
    }

    static private transient ArrayList<Execution>executions = new ArrayList<Execution>();
    
    static private void pushExecutions() {
        ArrayList<Execution> tmp = new ArrayList<Execution>();
        
        while( true ) {
            synchronized( executions ) {
                try { executions.wait(5000L); }
                catch( InterruptedException e ) { /* ignore me */ }
                if( executions.isEmpty() ) {
                    continue;
                }
                tmp.addAll(executions);
                executions.clear();
            }
            try {
                for( Execution execution : tmp ) {
                    Stack<Execution> stack;
                    
                    synchronized( cache ) {
                        if( !cache.containsKey(execution.getClass().getName()) ) {
                            stack = new Stack<Execution>();
                            cache.put(execution.getClass().getName(), stack);
                        }
                        else {
                            stack = cache.get(execution.getClass().getName());
                        }
                    }
                    synchronized( stack ) {
                        if( stack.size() > 10 || stack.contains(execution) ) {
                            continue;
                        }
                        stack.push(execution);
                    }
                }
            }
            catch( Throwable t ) {
                logger.error("Problem pushing executions: " + t.getMessage(), t);
            }
            finally {
                tmp.clear();                
            }
        }
    }

    
    /**
     * The connection to be used for the execution of this event.
     */
    public Connection        connection = null;
    /**
     * The data to be used for the execution of this event.
     */
    public Map<String,Object> data       = null;
    
    public String             dsn        = null;
    @Deprecated
    public ResultSet         results    = null;
    private String            state      = "IDLE";
    /**
     * The prepared statement that will execute the event.
     */
    public PreparedStatement statement  = null;

    public Execution() {
        super();
        dsn = getDataSourceName(getClass().getName(), isReadOnly());
    }
    
    /**
     * Closes out the event and returns it to the shared stack.
     */
    public void close() {
        logger.debug("enter - close()");
        try {
            connection = null;
            data = null;        
            synchronized( executions ) {
                executions.add(this);
                executions.notifyAll();
            }
        }
        finally {
            logger.debug("exit - close()");
        }
    }

    public String getIdentifier(String val) throws SQLException {
        String quotes = getQuotes();
        
        if( isUpperCase() ) {
            val = val.toUpperCase();
        }
        return quotes + val + quotes;
    }
    
    public String getIdentifier(String tbl, String col) throws SQLException {
        String quotes = getQuotes();
        
        if( isUpperCase() ) {
            tbl = tbl.toUpperCase();
            col = col.toUpperCase();
        }
        return quotes + tbl + quotes + "." + quotes + col + quotes;
    }
    
    public String getQuotes() throws SQLException {
        if( connection != null ) {
            return connection.getMetaData().getIdentifierQuoteString();
        }
        return "";
    }
    
    /**
     * Executes this event in the specified transaction context.
     * @param trans the transaction context
     * @param args the data to be used in the event
     * @return the results of the transaction
     * @throws org.dasein.persist.PersistenceException an error occurred
     * executing the event
     * @deprecated do not make direct calls to this method
     */
    public HashMap<String,Object> execute(Transaction trans, Map<String,Object> args) throws PersistenceException {
        Map<String,Object> r = executeEvent(trans, args, null);
        
        if( r instanceof HashMap ) {
            return (HashMap<String,Object>)r;
        }
        else {
            HashMap<String,Object> tmp = new HashMap<String,Object>();
            
            tmp.putAll(r);
            return tmp;
        }
    }
    
    /*
    Map<String,Object> executeEvent(Transaction trans, Map<String,Object> args) throws PersistenceException {
        return executeEvent(trans, args, null);
    }
    */
    
    Map<String,Object> executeEvent(Transaction trans, Map<String,Object> args, StringBuilder statementHolder) throws PersistenceException {
        logger.debug("enter - execute(Transaction, Map)");
        state = "EXECUTING";
        try {
            if( logger.isDebugEnabled() ) {
                logger.debug("Getting connection from transaction: " + trans.getTransactionId());
            }
            connection = trans.getConnection();
            data = args;
            try {
                String sql = loadStatement(connection, args);
                Map<String,Object> res;
                
                if( logger.isDebugEnabled() ) {
                    logger.debug("Preparing: " + sql);
                }
                if( statementHolder != null ) {
                    statementHolder.append(sql);
                }
                statement = connection.prepareStatement(sql);
                try {
                    logger.debug("And executing the prepared statement.");
                
                    res = run(trans, args);
                }
                finally {
                    try { statement.close(); statement = null; }
                    catch( Throwable ignore ) { }
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("RESULTS: " + res);
                }
                if( res ==  null ) {
                    return null;
                }
                else if( res instanceof HashMap ) {
                    return (HashMap<String,Object>)res;
                }
                else {
                    HashMap<String,Object> tmp = new HashMap<String,Object>();
                    
                    tmp.putAll(res);
                    return tmp;
                }
            }
            catch( SQLException e ) {
                logger.debug("Error executing event: " + e.getMessage(), e);
                throw new PersistenceException(e.getMessage());
            }
        }
        finally {
            state = "IDLE";
            logger.debug("exit - execute(Transaction, Map)");
        }
    }

    public Connection getConnection() {
        return connection;
    }
    
    /**
     * Events should implement this method to provide the name of the
     * data source that will be used to create connections for this event.
     * @return the name of the JDBC data source
     * @throws javax.naming.NamingException an error occurred providing
     * the name of the data source
     */
    public String getDataSource() throws NamingException {
        return dsn;
    }

    /**
     * @return EXECUTING or IDLE
     */
    public final String getState() {
        return state;
    }
    
    /**
     * @return the SQL for this event
     */
   public String getStatement() throws SQLException {
        return null;
    }

    public String getStatement(Connection conn) throws SQLException {
        return getStatement();
    }
    
    public String getStatement(Connection conn, Map<String,Object> params) throws SQLException {
        return getStatement(conn);
    }
    
    synchronized final String loadStatement(Connection conn, Map<String,Object> params) throws SQLException {
        boolean wasnull = false;
        
        if( connection == null ) {
            wasnull = true;
            connection = conn;
        }
        try {
            return getStatement(conn, params);
        }
        finally {
            if( wasnull ) {
                connection = null;
            }
        }
        
    }
    
    public boolean isReadOnly() {
        return false;
    }
    
    public boolean isUpperCase() throws SQLException {
        String dbms = connection.getMetaData().getDatabaseProductName();
        
        if( dbms == null ) {
            return false;
        }
        dbms = dbms.toLowerCase();
        return dbms.startsWith("hsql");
    }
    
    @SuppressWarnings("unchecked")
    public Map<String,Translator<String>> loadStringTranslations(Transaction xaction, Class cls, String id) throws PersistenceException, SQLException {
        Map<String,Object> criteria = new HashMap<String,Object>();
        Map<String,Translator<String>> map = new HashMap<String,Translator<String>>();
        
        criteria.put("ownerClass", cls);
        criteria.put("ownerId", id);
        criteria = xaction.execute(LoadTranslator.class, criteria, Execution.getDataSourceName(cls.getName()));
        // a retarded side-effect of the lame-ass implementation of generics in Java
        for( String attr : criteria.keySet() ) {
            Object trans = criteria.get(attr);
            
            map.put(attr, (Translator<String>)trans);
        }
        return map;
    }
    
    public void removeStringTranslations(Transaction xaction, Class cls, String id) throws PersistenceException, SQLException {
        Map<String,Object> state = new HashMap<String,Object>();
    
        state.put("ownerClass", cls);
        state.put("ownerId", id);
        xaction.execute(RemoveTranslator.class, state, Execution.getDataSourceName(cls.getName()));
    }
    
    /**
     * Event implementations should implement this method to process
     * the actual event.
     * @return the results of the event
     * @throws org.dasein.persist.PersistenceException a non-JDBC error
     * occurred executing the event
     * @throws java.sql.SQLException a JDBC error occurred executing the event
     * @deprecated use {@link #run(Transaction, Map)}
     */
    public Map<String,Object> run() throws PersistenceException, SQLException {
        // NO-OP
        return null;
    }

    public Map<String,Object> run(Transaction xaction, Map<String,Object> params) throws PersistenceException, SQLException {
        return run();
    }
    
    public void saveStringTranslation(Transaction xaction, Class cls, String id, String attr, Translator<String> t) throws SQLException, PersistenceException {
        saveStringTranslation(xaction, cls.getName(), id, attr, t);
    }
    
    public void saveStringTranslation(Transaction xaction, String cname, String id, String attr, Translator<String> t) throws SQLException, PersistenceException {
        Map<String,Object> state = new HashMap<String,Object>();
        
        state.put("ownerClass", cname);
        state.put("ownerId", id);
        state.put("attribute", attr);
        state.put("translation", t);
        xaction.execute(SaveTranslator.class, state, Execution.getDataSourceName(cname));
    }
    
    /**
     * Method to set the data to be used in the transaction.
     * @param data the data to be set
     */
    public void setData(HashMap<String,Object> data) {
        this.data = data;
    }
    
    protected void setDsn(String dsn) {
        this.dsn = dsn;
    }
}

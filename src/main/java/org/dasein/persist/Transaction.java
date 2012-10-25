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

/* $Id: Transaction.java,v 1.14 2009/07/02 01:36:20 greese Exp $ */
/* Copyright (c) 2002-2004 Valtira Corporation, All Rights Reserved */
package org.dasein.persist;

// J2SE imports
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

// J2EE imports
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

// Apache imports
import org.apache.log4j.Logger;

/**
 * Represents a database transaction to applications managing their
 * own transactions. To use this class, call <code>getInstance()</code>
 * to get a transaction and pass that transaction to all
 * <code>Execution</code> objects that need to execute in the same
 * transaction context.
 * <br/>
 * Last modified: $Date: 2009/07/02 01:36:20 $
 * @version $Revision: 1.14 $
 * @author George Reese
 */
public class Transaction {
    static private Logger logger            = Logger.getLogger(Transaction.class);

    /**
     * A count of open connections.
     */
    static private int connections = 0;
    /**
     * The most transactions ever open at a single time.
     */
    static private int highPoint   = 0;
    /**
     * The maid service cleans up dead transactions.
     */
    static private Thread maid      = null;
    /**
     * The next transaction ID to use.
     */
    static private int nextTransaction = 1;
    /**
     * A list of open transactions.
     */
    static private Map<Number,Transaction> transactions = new HashMap<Number,Transaction>();
    
    static private Map<String,Stack<Execution>> eventCache = new HashMap<String,Stack<Execution>>();
    
    /**
     * Cleans up transactions that somehow never got cleaned up.
     */
    static private void clean() {
        while( true ) {
            ArrayList<Transaction> closing;
            int count;
            
            if( logger.isInfoEnabled() ) {
                logger.info("There are " + connections + " open connections right now (high point: " + highPoint + ").");
            }
            try { Thread.sleep(1000L); }
            catch( InterruptedException ignore ) { /* ignore me */ }
            count = transactions.size();
            if( count < 1 ) {
                continue;
            }
            if( logger.isInfoEnabled() ) {
                logger.info("Running the maid service on " + count + " transactions.");
            }
            closing = new ArrayList<Transaction>();
            synchronized( transactions ) {
                long now = System.currentTimeMillis();

                for( Transaction xaction : transactions.values() ) {
                    long diff = (now - xaction.openTime)/1000L;
                    
                    if( diff > 10 ) {
                        Thread t = xaction.executionThread;
                        
                        logger.warn("Open transaction " + xaction.transactionId + " has been open for " + diff + " seconds.");
                        logger.warn("Transaction " + xaction.transactionId + " state: " + xaction.state);
                        if( t== null ) {
                            logger.warn("Thread: no execution thread active");
                        }
                        else {
                            logger.warn("Thread " + t.getName() + " (" + t.getState() + "):");
                            for( StackTraceElement elem : t.getStackTrace() ) {
                                logger.warn("\t" + elem.toString());
                            }
                        }
                    }
                    if( diff  > 600 ) {
                        closing.add(xaction);
                    }
                }
            }
            for( Transaction xaction: closing ) {
                logger.warn("Encountered a stale transaction (" + xaction.transactionId + "), forcing a close: " + xaction.state);
                xaction.printStackTrace();
                xaction.close();
            }
        }
    }
    
    /**
     * Provides a new transaction instance to manage your transaction
     * context.
     * @return the transaction for your new transaction context
     */
    static public Transaction getInstance() {
        return getInstance(false);
    }
    
    static public Transaction getInstance(boolean readOnly) {        
        Transaction xaction;
        int xid;

        if( nextTransaction == Integer.MAX_VALUE ) {
            nextTransaction = 0;
        }
        xid = nextTransaction++;
        xaction = new Transaction(xid, readOnly);
        if( maid == null ) {
            synchronized( transactions ) { 
                // this bizarreness avoids synchronizing for most cases
                if( maid == null ) {
                    maid = new Thread() {
                        public void run() {
                            clean();
                        }   
                    };  
                    maid.setDaemon(true);
                    maid.setPriority(Thread.MIN_PRIORITY + 1);
                    maid.setName("Transaction Maid");
                    maid.start();
                }
            }
        }
        return xaction;
    }

    static public void report() {
        MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
        
        System.out.println("\n\nDasein Connection Report (" + new Date() + "):");
        System.out.println("\tOpen connections: " + connections);
        System.out.println("\tHigh connections: " + highPoint);
        System.out.println("\tTransaction cache size: " + transactions.size());
        System.out.println("\tEvent cache size: " + eventCache.size());
        System.out.println("\tHeap memory usage: " + bean.getHeapMemoryUsage());
        System.out.println("\tNon-heap memory usage: " + bean.getNonHeapMemoryUsage());
        System.out.println("\tFree memory: " + (Runtime.getRuntime().freeMemory()/1024000) + "MB");
        System.out.println("\tTotal memory: " + (Runtime.getRuntime().totalMemory()/1024000) + "MB");
        System.out.println("\tMax memory: " + (Runtime.getRuntime().maxMemory()/1024000) + "MB");
        System.out.println("\n");
    }
    
    /**
     * A connection object for this transaction.
     */
    private Connection connection = null;
    /**
     * Marks the transaction as dirty and no longer able to be used.
     */
    private boolean    dirty      = false;
    
    private Thread executionThread = null;
    /**
     * A stack of events that are part of this transaction.
     */
    private Stack<Execution> events     = new Stack<Execution>();
    
    private Stack<String> statements = new Stack<String>();
    /**
     * Marks the time the transaction was opened so it can be closed.
     */
    private long       openTime   = 0L;
    /**
     * Defines this transaction as being a read-only transaction.
     */
    private boolean    readOnly   = false;
    /**
     * A state tracker for debugging purposes.
     */
    private String     state      = "NEW";
    /**
     * A unique transaction identifier.
     */
    private int        transactionId;

    private StackTraceElement[] stackTrace;
    
    /**
     * Constructs a transaction object having the specified transaction ID.
     * @param xid the transaction ID that identifies the transaction
     * @param readOnly true if the transaction is just for reading.
     */
    private Transaction(int xid, boolean readOnly) {
        super();
        transactionId = xid;
        this.readOnly = readOnly;
    }
    
    /**
     * Closes the transaction. If the transaction has not been committed,
     * it is rolled back.
     */
    public void close() {
        logger.debug("enter - close()");
        try {
            state = "CLOSING";
            if( connection != null ) {
                logger.warn("Connection not committed, rolling back.");
                rollback();
            }
            logger.debug("Closing all open events.");
            if( !events.empty() ) {
                state = "CLOSING EVENTS";
                do {
                    Execution exec = (Execution)events.pop();
                
                    try {
                        Stack<Execution> stack;
                        
                        exec.close();
                        stack = eventCache.get(exec.getClass().getName());
                        if( stack != null ) {
                            stack.push(exec);
                        }
                    }
                    catch( Throwable t ) {
                        t.printStackTrace();
                    }
                } while( !events.empty() );
            }
            state = "CLOSED";
            logger.debug("return - close()");
        }
        finally {
            if( logger.isDebugEnabled() ) {
                logger.debug("Removing transaction: " + transactionId);
            }
            synchronized( transactions ) {
                transactions.remove(new Integer(transactionId));
            }
            logger.debug("exit - close()");
            events.clear();
            statements.clear();
            stackTrace = null;
        }
    }

    /**
     * Commits the transaction to the database and closes the transaction.
     * The transaction should not be used or referenced after calling
     * this method.
     * @throws org.dasein.persist.PersistenceException either you are trying
     * to commit to a used up transaction or a database error occurred
     * during the commit
     */
    public void commit() throws PersistenceException {
        logger.debug("enter - commit()");
        try {
            if( connection == null ) {
                if( dirty ) {
                    throw new PersistenceException("Attempt to commit a committed or aborted transaction.");
                }       
                logger.debug("return as no-op - commit()");
                return;
            }
            state = "COMMITTING";
            try {
                if( logger.isDebugEnabled() ) {
                    logger.debug("Committing: " + transactionId);
                }
                connection.commit();
                state = "CLOSING CONNECTIONS";
                connection.close();
                connection = null;
                if( logger.isInfoEnabled() ) {
                    logger.info("Reducing the number of connections from " + connections + " due to commit.");
                }
                connections--;
                if( logger.isDebugEnabled() ) {
                    logger.debug("Releasing: " + transactionId);
                }
                close();
                logger.debug("return - commit()");
            }
            catch( SQLException e ) {
                throw new PersistenceException(e.getMessage());
            }
            finally {
                if( connection != null ) {
                    logger.warn("Commit failed: " + transactionId);
                    rollback();
                }
                dirty = true;
            }
        }
        finally {
            logger.debug("exit - commit()");
        }
    }

    public Map<String,Object> execute(Class<? extends Execution> cls, Map<String,Object> args) throws PersistenceException {
        return execute(cls, args, null);
    }
    
    public Map<String,Object> execute(Class<? extends Execution> cls, Map<String,Object> args, String dsn) throws PersistenceException {
        logger.debug("enter - execute(Class,Map)");
        try {
            StringBuilder holder = new StringBuilder();
            boolean success = false;
            
            executionThread = Thread.currentThread();
            state = "PREPARING";
            try {
                Execution event = getEvent(cls);
                Map<String,Object> res;
                
                if( connection == null ) {
                    if( logger.isDebugEnabled() ) {
                        logger.debug("New connection: " + transactionId);
                    }
                    open(event, dsn);
                }
                /*
                stateargs = event.loadStatement(connection, args);
                if( stateargs == null ) {
                    state = "EXECUTING";
                }
                else {
                    state = "EXECUTING:\n" + stateargs;
                }
                */
                state = "EXECUTING " + event.getClass().getName();
                stackTrace = Thread.currentThread().getStackTrace();
                res = event.executeEvent(this, args, holder);
                events.push(event);
                statements.push(holder.toString());
                success = true;
                logger.debug("return - execute(Execution, Map)");
                state = "AWAITING COMMIT: " + holder.toString();
                return res;
            }
            catch( SQLException e ) {
                logger.warn("SQLException: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new PersistenceException(e);
            }
            catch( InstantiationException e ) {
                logger.error("Instantiation exception: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new PersistenceException(e);
            }
            catch( IllegalAccessException e ) {
                logger.error("IllegalAccessException: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new PersistenceException(e);
            }
            catch( RuntimeException e ) {
                logger.error("RuntimeException: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                e.printStackTrace();
                throw new PersistenceException(e);
            }
            catch( Error e ) {
                logger.error("Error: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new PersistenceException(new RuntimeException(e));
            }
            finally {
                if( !success ) {
                    logger.warn("FAILED TRANSACTION (" + transactionId + "): " + holder.toString());
                    rollback();
                }
                executionThread = null;
            }
        }
        finally {
            logger.debug("exit - execute(Class,Map)");
        }
    }
    
    public Map<String,Object> execute(Execution event, Map<String,Object> args, String dsn) throws PersistenceException {
        logger.debug("enter - execute(Class,Map)");
        try {
            StringBuilder holder = new StringBuilder();
            boolean success = false;
            
            executionThread = Thread.currentThread();
            state = "PREPARING";
            try {
                Map<String,Object> res;
                
                if( connection == null ) {
                    if( logger.isDebugEnabled() ) {
                        logger.debug("New connection: " + transactionId);
                    }
                    open(event, dsn);
                }
                //stateargs = event.loadStatement(connection, args);
                state = "EXECUTING " + event.getClass().getName();
                stackTrace = Thread.currentThread().getStackTrace();
                res = event.executeEvent(this, args, holder);
                events.push(event);
                statements.push(holder.toString());
                success = true;
                state = "AWAITING COMMIT: " + holder.toString();
                logger.debug("return - execute(Execution, Map)");
                return res;
            }
            catch( SQLException e ) {
                logger.warn("SQLException: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new PersistenceException(e);
            }
            catch( RuntimeException e ) {
                logger.error("RuntimeException: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                e.printStackTrace();
                throw new PersistenceException(e);
            }
            catch( Error e ) {
                logger.error("Error: " + e.getMessage());
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new PersistenceException(new RuntimeException(e));
            }
            finally {
                if( !success ) {
                    logger.warn("FAILED TRANSACTION (" + transactionId + "): " + holder.toString());
                    rollback();
                }
                executionThread = null;
            }
        }
        finally {
            logger.debug("exit - execute(Class,Map)");
        }
    }
    
    /**
     * Executes the specified event as part of this transaction.
     * @param event the event to execute in this transaction context
     * @param args the values to be used by the event
     * @return results from the transaction, can be null
     * @throws org.dasein.persist.PersistenceException an error occurred
     * interacting with the database
     * @deprecated use {@link #execute(Class, Map)}
     */
    public HashMap<String,Object> execute(Execution event, Map<String,Object> args) throws PersistenceException {
        Map<String,Object> r = execute(event.getClass(), args);
        
        if( r == null ) {
            return null;
        }
        if( r instanceof HashMap ) {
            return (HashMap<String,Object>)r;
        }
        else {
            HashMap<String,Object> tmp = new HashMap<String,Object>();
            
            tmp.putAll(r);
            return tmp;
        }
    }

    /**
     * Provides events with access to the connection supporting this
     * transaction.
     * @return the connection supporting this transaction
     */
    public Connection getConnection() {
        return connection;
    }

    private Execution getEvent(Class<? extends Execution> cls) throws InstantiationException, IllegalAccessException {
        Stack<Execution> stack = eventCache.get(cls.getName());
        Execution event;
        
        if( stack != null ) {
            // by not checking for empty, we can skip synchronization
            try { event = stack.pop(); }
            catch( EmptyStackException e ) { event = null; }
            if( event != null ) {
                return event;
            }
        }
        else {
            stack = new Stack<Execution>();
            eventCache.put(cls.getName(), stack);
        }
        event = cls.newInstance();
        return event;
    }
    
    /**
     * Each transaction has a number that helps identify it for debugging purposes.
     * @return the identifier for this transaction
     */
    public int getTransactionId() {
        return transactionId;
    }
    
    /**
     * @return a unique hash code
     */
    public int hashCode() {
        return transactionId;
    }
    
    /**
     * Opens a connection for the specified execution.
     * @param event the event causing the open
     * @throws SQLException
     * @throws PersistenceException
     */
    private synchronized void open(Execution event, String dsn) throws SQLException, PersistenceException {
        logger.debug("enter - open(Execution)");
        try {
            Connection conn;
            
            if( connection != null ) {
                return;
            }
            state = "OPENING";
            openTime = System.currentTimeMillis();
            if( logger.isDebugEnabled() ) {
                logger.debug("Opening " + transactionId);
            }
            try {
                InitialContext ctx = new InitialContext();
                DataSource ds;
                
                if( dsn == null ) {
                    dsn = event.getDataSource();
                }
                state = "LOOKING UP";
                ds = (DataSource)ctx.lookup(dsn);
                conn = ds.getConnection();
                openTime = System.currentTimeMillis();
                if( logger.isDebugEnabled() ) {
                    logger.debug("Got connection for " + transactionId + ": " + conn);
                }            
                state = "CONNECTED";
            }
            catch( NamingException e ) {
                e.printStackTrace();
                throw new PersistenceException(e.getMessage());
            }
            conn.setAutoCommit(false);
            conn.setReadOnly(readOnly);
            connection = conn;
            if( logger.isInfoEnabled() ) {
                logger.info("Incrementing connection count from " + connections);
            }
            connections++;
            if( connections > highPoint ) {
                highPoint = connections;
                if( logger.isInfoEnabled() ) {
                    logger.info("A NEW CONNECTION HIGH POINT HAS BEEN REACHED: " + highPoint);
                }
            }
            synchronized( transactions ) {
                transactions.put(new Integer(transactionId), this);
            }
            logger.debug("return - open(Execution)");
        }
        finally {
            logger.debug("exit - open(Execution)");
        }
    }
    
    private void printElement(StackTraceElement element) {
        int no = element.getLineNumber();
        String ln;
        
        if( no < 10 ) {
            ln = "     " + no;
        }
        else if( no < 100 ) {
            ln = "    " + no;
        }
        else if( no < 1000 ) {
            ln = "   " + no;
        }
        else if( no < 10000 ) {
            ln = "  " + no;
        }
        else {
            ln = " " + no;
        }
        System.out.println("\t" + ln + " " + element.getFileName() + ": " + element.getClassName() + "." + element.getMethodName());
    }
    
    private void printStackTrace() {
        if( stackTrace == null ) {
            System.out.println("\t--> No stack trace <--");
        }
        else {
            for( StackTraceElement element : stackTrace ) {
                printElement(element);
            }
        }
    }
    /**
     * Rolls back the transaction and closes this transaction. The
     * transaction should no longer be referenced after this point.
     */
    public void rollback() {
        logger.debug("enter - rollback()");
        try {
            if( connection == null ) {
                return;
            }
            state = "ROLLING BACK";
            logger.debug("Rolling back JDBC connection: " + transactionId);
            try { connection.rollback(); }
            catch( SQLException e ) { e.printStackTrace(); }
            try { connection.close(); }
            catch( SQLException e ) { e.printStackTrace(); }
            connection = null;
            if( logger.isInfoEnabled() ) {
                logger.info("Reducing the number of connections from " + connections + " due to rollback.");
            }
            connections--;
            close();
            dirty = true;
            logger.debug("return - rollback()");
        }
        finally {
            logger.debug("exit - rollback()");
        }
    }
}

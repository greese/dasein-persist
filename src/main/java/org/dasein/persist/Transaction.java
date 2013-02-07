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
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    static private final Logger logger = Logger.getLogger(Transaction.class);

    /**
     * A count of open connections.
     */
    static private final AtomicInteger connections = new AtomicInteger(0);
    /**
     * The most transactions ever open at a single time.
     */
    static private final AtomicInteger highPoint = new AtomicInteger(0);
    /**
     * The next transaction ID to use.
     */
    static private final AtomicInteger nextTransactionId = new AtomicInteger(0);
    /**
     * A list of open transactions.
     */
    static private final Map<Number,Transaction> transactions = new ConcurrentHashMap<Number, Transaction>(8, 0.9f, 1);
    /**
     * Cache of Execution objects to minimize dynamically created SQL
     */
    static private final Map<String,Stack<Execution>> eventCache = new ConcurrentHashMap<String, Stack<Execution>>(8, 0.9f, 1);

    static private final AtomicBoolean maidLaunched = new AtomicBoolean(false);
    static private final Properties properties = new Properties();

    static public final String ALTMAID_ENABLED = "dasein.persist.altmaid.enabled";
    static public final String ALTMAID_FREQUENCY = "dasein.persist.altmaid.frequency";
    static public final String ALTMAID_MAXSECONDS = "dasein.persist.altmaid.maxseconds";
    static public final String ALTMAID_WARNSECONDS = "dasein.persist.altmaid.warnseconds";


    /**
     * Cleans up transactions that somehow never got cleaned up.
     */
    static private void clean() {
        while( true ) {
            ArrayList<Transaction> closing;
            int count;
            
            if( logger.isInfoEnabled() ) {
                logger.info("There are " + connections.get() + " open connections right now (high point: " + highPoint.get() + ").");
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
            long now = System.currentTimeMillis();

            // ConcurrentHashMap provides a weakly-consistent view for this iterator
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
            for( Transaction xaction: closing ) {
                logger.warn("Encountered a stale transaction (" + xaction.transactionId + "), forcing a close: " + xaction.state);
                xaction.printStackTrace();
                xaction.close();
            }
        }
    }

    /**
     * Alternate way to clean up transactions.
     */
    static private void altClean() {
        int cycleCount = 0;
        long warn = getAltMaidWarnMs();
        long max = getAltMaidMaxMs();
        long freq = getAltMaidFrequencyMs();
        while (true) {
            if (cycleCount % 21 == 20) {
                warn = getAltMaidWarnMs();
                max = getAltMaidMaxMs();
                freq = getAltMaidFrequencyMs();
            }
            try {
                Thread.sleep(freq);
                _altClean(warn, max);
            } catch (Throwable t) {
                logger.error("Problem running altmaid: " + t.getMessage(), t);
            } finally {
                cycleCount += 1;
            }
        }
    }
    static private void _altClean(long warnMs, long maxMs) {
        if (transactions.isEmpty()) {
            return;
        }
        final long now = System.currentTimeMillis();
        // ConcurrentHashMap provides a weakly-consistent view for this iterator
        for (Transaction xaction : transactions.values()) {
            final long diff = now - xaction.openTime;
            if (diff > maxMs) {
                logger.error("Transaction " + xaction.transactionId + " has been open for " + diff/1000L + " seconds, forcing a close: " + xaction.state);
                try {
                    xaction.rollback(true);
                    xaction.close();
                } catch (Throwable t) {
                    logger.error("Problem rolling back offending transaction " + xaction.transactionId + ": " + t.getMessage());
                }
            } else if (diff > warnMs) {
                logger.warn("Transaction " + xaction.transactionId + " has been open for " + diff/1000L + " seconds.");
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
        final int xid = nextTransactionId.incrementAndGet();
        if( xid == Integer.MAX_VALUE ) {
            nextTransactionId.set(0);
        }
        xaction = new Transaction(xid, readOnly);
        if (maidLaunched.compareAndSet(false, true)) {
            final Thread maid;
            loadProperties();
            if (isAltMaid()) {
                if (isAltMaidEnabled()) {
                    maid = new Thread() {
                        public void run() {
                            altClean();
                        }
                    };
                    maid.setName("Transaction Alt-Maid");
                } else {
                    maid = null;
                    logger.warn("Transaction maid disabled");
                }
            } else {
                maid = new Thread() {
                    public void run() {
                        clean();
                    }
                };
                maid.setName("Transaction Maid");
            }
            if (maid != null) {
                maid.setDaemon(true);
                maid.setPriority(Thread.MIN_PRIORITY + 1);
                maid.start();
            }
        }
        return xaction;
    }

    static public void report() {
        MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
        
        System.out.println("\n\nDasein Connection Report (" + new Date() + "):");
        System.out.println("\tOpen connections: " + connections.get());
        System.out.println("\tHigh connections: " + highPoint.get());
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
    private volatile Connection connection = null;
    /**
     * Marks the transaction as dirty and no longer able to be used.
     */
    private volatile boolean dirty = false;
    
    private volatile Thread executionThread = null;
    /**
     * A stack of events that are part of this transaction.
     */
    private final Stack<Execution> events = new Stack<Execution>();
    
    private final Stack<String> statements = new Stack<String>();
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
            transactions.remove(new Integer(transactionId));
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
                logger.warn(connectionCloseLog());
                final int numConnections = connections.decrementAndGet();
                if( logger.isInfoEnabled() ) {
                    logger.info("Reduced the number of connections from " + (numConnections+1) + " due to commit.");
                }
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
                logger.warn("DPTRANSID-" + transactionId + " connection.get - dsn='" + dsn + '\'');
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
            final int numConnections = connections.incrementAndGet();
            final int numHighPoint = highPoint.get();
            if( logger.isInfoEnabled() ) {
                logger.info("Incremented connection count to " + numConnections);
            }
            if( numConnections > numHighPoint) {
                if (highPoint.compareAndSet(numHighPoint, numConnections)) {
                    if( logger.isInfoEnabled() ) {
                        logger.info("A NEW CONNECTION HIGH POINT HAS BEEN REACHED: " + highPoint);
                    }
                }
            }
            transactions.put(new Integer(transactionId), this);
            logger.debug("return - open(Execution)");
        }
        finally {
            logger.debug("exit - open(Execution)");
        }
    }

    private String connectionCloseLog() {
        String log = "DPTRANSID-" + transactionId + " connection.close - duration=" + (System.currentTimeMillis() - openTime) + "ms - stmt='";
        String stmt = statements.peek();
        if (stmt != null) {
            if (stmt.length() < 100) {
                log = log + stmt.substring(0,stmt.length());
            } else {
                log = log + stmt.substring(0,100);
            }
        }
        return log + '\'';
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
        rollback(false);
    }

    protected void rollback(boolean cancelStatements) {

        logger.debug("enter - rollback()");
        try {
            if( connection == null ) {
                return;
            }
            if (cancelStatements) {
                int numCancelled = 0;
                for (Execution event : events) {
                    final Statement stmt = event.statement;
                    if (stmt != null) {
                        try {
                            if (!stmt.isClosed()) {
                                stmt.cancel();
                                numCancelled += 1;
                            }
                        } catch (Throwable t) {
                            logger.error("Problem cancelling statement: " + t.getMessage());
                        }
                    }
                }
                if (numCancelled > 0) {
                    if (numCancelled == 1) {
                        logger.warn("Cancelled 1 query");
                    } else {
                        logger.warn("Cancelled " + numCancelled + " queries");
                    }
                }
            }
            state = "ROLLING BACK";
            logger.debug("Rolling back JDBC connection: " + transactionId);
            try {
                connection.rollback();
            }
            catch( SQLException e ) {
                logger.error("Problem with rollback: " + e.getMessage(), e);
            }
            try {
                connection.close();
                logger.warn(connectionCloseLog());
            }
            catch( SQLException e )
            {
                logger.error("Problem closing connection: " + e.getMessage(), e);
            }
            connection = null;
            final int numConnections = connections.decrementAndGet();
            if( logger.isInfoEnabled() ) {
                logger.info("Reducing the number of connections from " + (numConnections+1) + " due to rollback.");
            }
            close();
            dirty = true;
            logger.debug("return - rollback()");
        }
        finally {
            logger.debug("exit - rollback()");
        }
    }

    static void loadProperties() {
        try {
            InputStream is = DaseinSequencer.class.getResourceAsStream(DaseinSequencer.PROPERTIES);
            try {
                if( is != null ) {
                    properties.load(is);
                }
            } finally {
                if( is != null ) {
                    is.close();
                }
            }
        } catch (Throwable t) {
            logger.error("Problem loading dasein persist transaction properties: " + t.getMessage());
        }
    }

    static boolean isAltMaid() {
        return properties.containsKey(ALTMAID_ENABLED);
    }

    static boolean isAltMaidEnabled() {
        return isAltMaid() && properties.getProperty(ALTMAID_ENABLED).equalsIgnoreCase("true");
    }

    static private long getMsFromSecondsProperty(String propKey, int defaultSeconds) {
        String secondsStr = properties.getProperty(propKey);
        if (secondsStr != null) {
            try {
                int seconds = Integer.parseInt(secondsStr);
                return (long)seconds * 1000L;
            } catch (NumberFormatException ignore) {
                logger.error("Value for '" + propKey + "' is not an integer, using default: " + defaultSeconds);
            }
        }
        return (long)defaultSeconds * 1000L;
    }

    static long getAltMaidFrequencyMs() {
        return getMsFromSecondsProperty(ALTMAID_FREQUENCY, 5);
    }

    static long getAltMaidWarnMs() {
        return getMsFromSecondsProperty(ALTMAID_WARNSECONDS, 10);
    }

    static long getAltMaidMaxMs() {
        return getMsFromSecondsProperty(ALTMAID_MAXSECONDS, 60);
    }
}

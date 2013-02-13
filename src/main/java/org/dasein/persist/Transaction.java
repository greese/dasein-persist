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
import org.dasein.util.DaseinUtilTasks;

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

    static public final String MAID_DISABLED = "dasein.persist.maid.disabled";
    static public final String MAID_FREQUENCY = "dasein.persist.maid.frequency";
    static public final String MAID_MAXSECONDS = "dasein.persist.maid.maxseconds";
    static public final String MAID_WARNSECONDS = "dasein.persist.maid.warnseconds";

    static private final boolean tracking;
    static {
        loadProperties();
        tracking = !isMaidDisabled();
    }

    /**
     * Clean up transactions.
     */
    private static class TransactionMaid implements Runnable {
        @Override
        public void run() {
            int cycleCount = 0;
            long warn = getMaidWarnMs();
            long max = getMaidMaxMs();
            long freq = getMaidFrequencyMs();
            while (true) {
                if (cycleCount % 21 == 20) {
                    warn = getMaidWarnMs();
                    max = getMaidMaxMs();
                    freq = getMaidFrequencyMs();
                }
                try {
                    Thread.sleep(freq);
                    _clean(warn, max);
                } catch (Throwable t) {
                    logger.error("Problem running transaction maid: " + t.getMessage(), t);
                } finally {
                    cycleCount += 1;
                }
            }
        }

        private static void _clean(long warnMs, long maxMs) {
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
        final int xid = nextTransactionId.incrementAndGet();
        if( xid == Integer.MAX_VALUE ) { // contention here will cause a few screwy values
            nextTransactionId.set(0);
        }
        Transaction xaction = new Transaction(xid, readOnly);
        if (maidLaunched.compareAndSet(false, true)) {
            loadProperties();
            if (!isMaidDisabled()) {
                DaseinUtilTasks.submit(new TransactionMaid());
            }
        }
        return xaction;
    }

    static public void report() {
        MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
        StringBuilder sb = new StringBuilder();
        sb.append("Dasein Connection Report (" + new Date() + "): ");
        if (tracking) {
            sb.append("Open connections: " + connections.get() + ", ");
            sb.append("Transaction cache size: " + transactions.size() + ", ");
        }
        sb.append("Event cache size: " + eventCache.size());
        sb.append(", Heap memory usage: " + bean.getHeapMemoryUsage());
        sb.append(", Non-heap memory usage: " + bean.getNonHeapMemoryUsage());
        sb.append(", Free memory: " + (Runtime.getRuntime().freeMemory() / 1024000L) + "MB");
        sb.append(", Total memory: " + (Runtime.getRuntime().totalMemory() / 1024000L) + "MB");
        sb.append(", Max memory: " + (Runtime.getRuntime().maxMemory() / 1024000L) + "MB");
        logger.info(sb.toString());
    }
    
    /**
     * A connection object for this transaction.
     */
    private volatile Connection connection = null;
    /**
     * Marks the transaction as dirty and no longer able to be used.
     */
    private volatile boolean dirty = false;
    
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
        try {
            state = "CLOSING";
            if( connection != null ) {
                logger.warn("Connection not committed, rolling back.");
                rollback();
            }
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
                        logger.error(t.getMessage(), t);
                    }
                } while( !events.empty() );
            }
            state = "CLOSED";
        }
        finally {
            if (tracking) {
                transactions.remove(new Integer(transactionId));
            }
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
        try {
            if( connection == null ) {
                if( dirty ) {
                    throw new PersistenceException("Attempt to commit a committed or aborted transaction.");
                }       
                return;
            }
            state = "COMMITTING";
            try {
                connection.commit();
                state = "CLOSING CONNECTIONS";
                connection.close();
                connection = null;
                if (logger.isDebugEnabled()) {
                    logger.debug(connectionCloseLog());
                }
                if (tracking) {
                    connections.decrementAndGet();
                }
                close();
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
        }
    }

    public Map<String,Object> execute(Class<? extends Execution> cls, Map<String,Object> args) throws PersistenceException {
        return execute(cls, args, null);
    }
    
    public Map<String,Object> execute(Class<? extends Execution> cls, Map<String,Object> args, String dsn) throws PersistenceException {
        try {
            StringBuilder holder = new StringBuilder();
            boolean success = false;
            
            state = "PREPARING";
            try {
                Execution event = getEvent(cls);
                Map<String,Object> res;
                
                if( connection == null ) {
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
                state = "AWAITING COMMIT: " + holder.toString();
                return res;
            }
            catch( SQLException e ) {
                String err = "SQLException: " + e.getMessage();
                if( logger.isDebugEnabled() ) {
                    logger.warn(err, e);
                } else {
                    logger.warn(err);
                }
                throw new PersistenceException(e);
            }
            catch( InstantiationException e ) {
                String err = "Instantiation exception: " + e.getMessage();
                if( logger.isDebugEnabled() ) {
                    logger.error(err, e);
                } else {
                    logger.error(err);
                }
                throw new PersistenceException(e);
            }
            catch( IllegalAccessException e ) {
                String err = "IllegalAccessException: " + e.getMessage();
                if( logger.isDebugEnabled() ) {
                    logger.error(err, e);
                } else {
                    logger.error(err);
                }
                throw new PersistenceException(e);
            }
            catch( RuntimeException e ) {
                logger.error("RuntimeException: " + e.getMessage(), e);
                throw new PersistenceException(e);
            }
            catch( Error e ) {
                String err = "Error: " + e.getMessage();
                if( logger.isDebugEnabled() ) {
                    logger.error(err, e);
                } else {
                    logger.error(err);
                }
                throw new PersistenceException(new RuntimeException(e));
            }
            finally {
                if( !success ) {
                    logger.warn("FAILED TRANSACTION (" + transactionId + "): " + holder.toString());
                    rollback();
                }
            }
        }
        finally {
        }
    }
    
    public Map<String,Object> execute(Execution event, Map<String,Object> args, String dsn) throws PersistenceException {
        try {
            StringBuilder holder = new StringBuilder();
            boolean success = false;
            
            state = "PREPARING";
            try {
                Map<String,Object> res;
                
                if( connection == null ) {
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
                return res;
            }
            catch( SQLException e ) {
                String err = "SQLException: " + e.getMessage();
                if( logger.isDebugEnabled() ) {
                    logger.warn(err, e);
                } else {
                    logger.warn(err);
                }
                throw new PersistenceException(e);
            }
            catch( RuntimeException e ) {
                logger.error("RuntimeException: " + e.getMessage(), e);
                throw new PersistenceException(e);
            }
            catch( Error e ) {
                String err = "Error: " + e.getMessage();
                if( logger.isDebugEnabled() ) {
                    logger.error(err, e);
                } else {
                    logger.error(err);
                }
                throw new PersistenceException(new RuntimeException(e));
            }
            finally {
                if( !success ) {
                    logger.warn("FAILED TRANSACTION (" + transactionId + "): " + holder.toString());
                    rollback();
                }
            }
        }
        finally {
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
        try {
            Connection conn;
            
            if( connection != null ) {
                return;
            }
            state = "OPENING";
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
                    logger.debug("DPTRANSID-" + transactionId + " connection.get - dsn='" + dsn + '\'');
                }
                state = "CONNECTED";
            }
            catch( NamingException e ) {
                logger.error("Problem with datasource: " + e.getMessage());
                throw new PersistenceException(e.getMessage());
            }
            conn.setAutoCommit(false);
            conn.setReadOnly(readOnly);
            connection = conn;
            if (tracking) {
                connections.incrementAndGet();
                transactions.put(new Integer(transactionId), this);
            }
        }
        finally {
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

    private String elementToString(StackTraceElement element) {
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
        return ln + " " + element.getFileName() + ": " + element.getClassName() + "." + element.getMethodName();
    }
    
    private void logStackTrace() {
        if( stackTrace == null ) {
            logger.error("--> No stack trace, ID " + transactionId +" <--");
        }
        else {
            StringBuilder sb = new StringBuilder("Stack trace for ").append(transactionId).append(":\n");
            for( StackTraceElement element : stackTrace ) {
                sb.append(elementToString(element)).append('\n');
            }
            logger.error(sb.toString());
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
            try {
                connection.rollback();
            }
            catch( SQLException e ) {
                logger.error("Problem with rollback: " + e.getMessage(), e);
            }
            try {
                connection.close();
                if (logger.isDebugEnabled()) {
                    logger.debug(connectionCloseLog());
                }
            }
            catch( SQLException e )
            {
                logger.error("Problem closing connection: " + e.getMessage(), e);
            }
            connection = null;
            if (tracking) {
                connections.decrementAndGet();
            }
            close();
            dirty = true;
        }
        finally {
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


    static boolean isMaidDisabled() {
        return properties.containsKey(MAID_DISABLED) && properties.getProperty(MAID_DISABLED).equalsIgnoreCase("true");
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

    static long getMaidFrequencyMs() {
        return getMsFromSecondsProperty(MAID_FREQUENCY, 5);
    }

    static long getMaidWarnMs() {
        return getMsFromSecondsProperty(MAID_WARNSECONDS, 10);
    }

    static long getMaidMaxMs() {
        return getMsFromSecondsProperty(MAID_MAXSECONDS, 60);
    }
}

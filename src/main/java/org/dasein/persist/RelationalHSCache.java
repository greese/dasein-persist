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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.dasein.persist.jdbc.AutomatedSql.Operator;
import org.dasein.persist.jdbc.AutomatedSql.TranslationMethod;
import org.dasein.persist.jdbc.Counter;
import org.dasein.persist.jdbc.Creator;
import org.dasein.persist.jdbc.Deleter;
import org.dasein.persist.jdbc.Loader;
import org.dasein.persist.jdbc.Updater;
import org.dasein.persist.l10n.LocalizationGroup;
import org.dasein.util.CacheLoader;
import org.dasein.util.CacheManagementException;
import org.dasein.util.CachedItem;
import org.dasein.util.JitCollection;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorFilter;
import org.dasein.util.Translator;

import com.google.code.hs4j.FindOperator;
import com.google.code.hs4j.HSClient;
import com.google.code.hs4j.IndexSession;
import com.google.code.hs4j.impl.HSClientImpl;

/**
 * <p>A relational AND HandlerSocket cache.  Only use this if you are running Percona MySQL!.</p>
 * 
 * <p>TODO:</p>
 * <ul>
 *   <li>Support more types of finds</li>
 *   <li>Supprot other CRUD operations</li>
 *   <li>Refactor to be more elegant with determining SQL names, etc</li>
 * </ul>
 * 
 * @author morgan
 *
 * @param <T>
 */
public final class RelationalHSCache<T extends CachedItem> extends PersistentCache<T> {
	static public final Logger logger = Logger.getLogger(RelationalCache.class);

    static public class OrderedColumn {
        public String  column;
        public boolean descending = false;
    }
    
    private String[] columns;
    private String[] databaseColumns;
    private String database;
    private String handlerSocketHost;
    private HSClient hsClient;	
	private Map<String,IndexSession> indexSessions = new HashMap<String,IndexSession>();
	private int poolSize;
	private int port;	
    private String readDataSource = null;
    private TranslationMethod translationMethod = TranslationMethod.NONE;
    private HashMap<String,Class<?>> types = new HashMap<String,Class<?>>();
    private String writeDataSource = null;
    
    public RelationalHSCache() { }
    
    /**
     * Constructs a new persistent factory for objects of the specified class with 
     * the named unique identifier attributes as well as with reading the persistence properties
     * file to find out the HandlerSocket host, database, port and pool size.
     * 
     * @param cls the class of objects managed by this factory
     * @param keys a list of unique identifiers for instances of the specified class
     */
    protected void init(Class<T> cls, Key ... keys) {
        readDataSource = Execution.getDataSourceName(cls.getName(), true);
        writeDataSource = Execution.getDataSourceName(cls.getName(), false);
        if( readDataSource == null ) {
            readDataSource = writeDataSource;
        }
        if( writeDataSource == null ) {
            writeDataSource = readDataSource;
        }
        
        Properties props = new Properties();
        
        try {
            InputStream is = DaseinSequencer.class.getResourceAsStream(DaseinSequencer.PROPERTIES);

            if( is != null ) {
                props.load(is);
            }
        }
        catch( Exception e ) {
            logger.error("Problem reading " + DaseinSequencer.PROPERTIES + ": " + e.getMessage(), e);
        }
        database = props.getProperty("dasein.persist.handlersocket.database");
        handlerSocketHost = props.getProperty("dasein.persist.handlersocket.host");
        port = Integer.valueOf(props.getProperty("dasein.persist.handlersocket.port"));
        poolSize = Integer.valueOf(props.getProperty("dasein.persist.handlersocket.poolSize"));
        
        // get the columns 
        List<String> targetColumns = new ArrayList<String>();
        Class<?> clazz = cls;
        while( clazz != null && !clazz.equals(Object.class) ) {
            Field[] fields = clazz.getDeclaredFields();
            
            for( Field f : fields ) {
                int m = f.getModifiers();
            
                if( Modifier.isTransient(m) || Modifier.isStatic(m) ) {
                    continue;
                }
                if (f.getType().getName().equals(Collection.class.getName())) {
                	continue; // this is handled by dependency manager
                }
                if( !f.getType().getName().equals(Translator.class.getName()) ) {
                	targetColumns.add(f.getName());    
                	types.put(f.getName(), f.getType());
                }
            }
            clazz = clazz.getSuperclass();
        }
        
        columns = new String[targetColumns.size()];
        databaseColumns = new String[targetColumns.size()];
        
        for (int i = 0; i < targetColumns.size(); i++) {
        	columns[i] = targetColumns.get(i);
        	databaseColumns[i] = getSqlName(targetColumns.get(i));
        }        
    }
    
    private Counter getCounter(SearchTerm[] whereTerms) {
        final SearchTerm[] terms = whereTerms;
        final RelationalHSCache<T> self = this;
        
        Counter counter = new Counter() {
            public void init() {
                setTarget(self.getEntityClassName());
                if( terms != null && terms.length > 0 ) {
                    ArrayList<Criterion> criteria = new ArrayList<Criterion>();
                
                    for( SearchTerm term : terms ) {
                        criteria.add(new Criterion(term.getColumn(), term.getOperator()));
                    }
                    setCriteria(criteria.toArray(new Criterion[criteria.size()]));
                }
            }
            
            public boolean isReadOnly() {
                return true;
            }
        };
        return counter;
    }
    
    private Creator getCreator() {
        final RelationalHSCache<T> self = this;
        
        Creator creator = new Creator() {
            public void init() {
                setTarget(self.getEntityClassName());
                switch (translationMethod) {
                case CUSTOM: setCustomTranslating(); break;
                case STANDARD: setTranslating(true); break;
                case NONE: setTranslating(false); break;
                }
            }
            
            public boolean isReadOnly() {
                return false;
            }
        };
        return creator;
    }
    
    private Deleter getDeleter() {
        final RelationalHSCache<T> self = this;
        
        Deleter deleter = new Deleter() {
            public void init() {
                setTarget(self.getEntityClassName());
                setCriteria(self.getPrimaryKey().getFields());
                switch (translationMethod) {
                case CUSTOM: setCustomTranslating(); break;
                case STANDARD: setTranslating(true); break;
                case NONE: setTranslating(false); break;
                }
            }
            
            public boolean isReadOnly() {
                return false;
            }
        };
        return deleter;
    }
    
    private Loader getLoader(SearchTerm[] whereTerms, OrderedColumn[] orderBy) {
        final SearchTerm[] terms = whereTerms;
        final OrderedColumn[] order = orderBy;
        final RelationalHSCache<T> self = this;
        
        Loader loader = new Loader() {
            public void init() {
                setTarget(self.getEntityClassName());
                setEntityJoins(getJoins());
                if( terms != null && terms.length > 0 ) {
                    ArrayList<Criterion> criteria = new ArrayList<Criterion>();
                
                    for( SearchTerm term : terms ) {
                        criteria.add(new Criterion(term.getJoinEntity(), term.getColumn(), term.getOperator()));
                    }
                    setCriteria(criteria.toArray(new Criterion[criteria.size()]));
                }
                if( order != null && order.length > 0 ) {
                    ArrayList<String> cols = new ArrayList<String>();
                    boolean desc = order[0].descending;
                    
                    for( OrderedColumn col : order ) {
                        cols.add(col.column);
                    }
                    setOrder(desc, cols.toArray(new String[cols.size()]));
                }
                switch (translationMethod) {
                case CUSTOM: setCustomTranslating(); break;
                case STANDARD: setTranslating(true); break;
                case NONE: setTranslating(false); break;
                }
            }
            
            public boolean isReadOnly() {
                return true;
            }
        };
        return loader;
    }
    
    private Updater getUpdater() {
        final RelationalHSCache<T> self = this;
        
        Updater updater = new Updater() {
            public void init() {
                setTarget(self.getEntityClassName());
                setCriteria(self.getPrimaryKey().getFields());
                switch (translationMethod) {
                case CUSTOM: setCustomTranslating(); break;
                case STANDARD: setTranslating(true); break;
                case NONE: setTranslating(false); break;
                }
            }
            
            public boolean isReadOnly() {
                return false;
            }
        };
        return updater;
    }
    
    /**
     * Counts the total number of objects governed by this factory in the database.
     * @return the number of objects in the database
     * @throws PersistenceException an error occurred counting the elements in the database
     */
    @Override
    public long count() throws PersistenceException {
        logger.debug("enter - count()");
        try {
            Transaction xaction = Transaction.getInstance(true);
            Counter counter = getCounter(null);            
            
            try {
                Map<String,Object> results;
                long count;
    
                results = xaction.execute(counter, new HashMap<String,Object>(), readDataSource);
                count = ((Number)results.get("count")).longValue();
                xaction.commit();
                return count;
            }
            finally {
                xaction.rollback();
            }
        }
        finally {
            logger.debug("exit - count()");
        }
    }
        
    @Override
    public long count(SearchTerm ... terms) throws PersistenceException {
        logger.debug("enter - count(SearchTerm...)");
        try {
            Transaction xaction = Transaction.getInstance(true);
            Counter counter = getCounter(terms);            
            
            try {
                Map<String,Object> params = toParams(terms);                
                Map<String,Object> results;
                long count;
    
                results = xaction.execute(counter, params, readDataSource);
                count = ((Number)results.get("count")).longValue();
                xaction.commit();
                return count;
            }
            finally {
                xaction.rollback();
            }
        }
        finally {
            logger.debug("exit - count(SearchTerm...)");
        }
    }
    
    /**
     * Creates the specified object with the data provided in the specified state under
     * the governance of the specified transaction.
     * @param xaction the transaction governing this event
     * @param state the new state for the new object
     * @throws PersistenceException an error occurred talking to the data store, or
     * creates are not supported
     */
    @Override
    public T create(Transaction xaction, Map<String,Object> state) throws PersistenceException {
        state.put("--key--", getPrimaryKey().getFields()[0]);
        xaction.execute(getCreator(), state, writeDataSource);
        return getCache().find(state);
    }
    
    @Override
    public Collection<T> find(SearchTerm[] terms, JiteratorFilter<T> filter, Boolean orderDesc, String ... orderFields) throws PersistenceException {
        logger.debug("enter - find(SearchTerm[], JiteratorFilter, Boolean, String)");
        try {
            OrderedColumn[] order;
            
            if( orderFields == null ) {
                order = new OrderedColumn[0];
            }
            else {
                int i = 0;
                
                order = new OrderedColumn[orderFields.length];
                for( String field : orderFields ) {
                    order[i] = new OrderedColumn();
                    order[i].column = field;
                    order[i].descending = (orderDesc != null && orderDesc);
                    i++;
                }
            }
            Loader loader = getLoader(terms, order);
            
            return this.load(loader, filter, toParams(terms));
        }
        finally {
            logger.debug("exit - find(SearchTerm[], JiteratorFilter, Boolean, String...)");
        }
    }
    
    /**
     * Retrieves the object uniquely identified by the value for the specified ID field.
     * @param primaryKeyValue the ID field identifying the object
     * @return the object matching the query criterion
     * @throws PersistenceException an error occurred talking to the data store
     */
    @Override
    public T get(Object primaryKeyValue) throws PersistenceException {
        try {
            CacheLoader<T> loader;
            
            loader = new CacheLoader<T>() {
                public T load(Object ... args) {
                    SearchTerm[] terms = new SearchTerm[1];
                    Collection<T> list;
                    
                    terms[0] = new SearchTerm((String)args[0], Operator.EQUALS, args[1]);
                    try {
                        list = RelationalHSCache.this.load(getLoader(terms, null), null, toParams(terms));
                    }
                    catch( PersistenceException e ) {
                        try {
                            try { Thread.sleep(1000L); }
                            catch( InterruptedException ignore ) { }
                            list = RelationalHSCache.this.load(getLoader(terms, null), null, toParams(terms));
                        }
                        catch( Throwable forgetIt ) {
                            logger.error(forgetIt.getMessage(), forgetIt);
                            throw new RuntimeException(e);
                        }
                    }
                    if( list.isEmpty() ) {
                        return null;
                    }
                    return list.iterator().next();
                }
            };
            logger.debug("Executing cache find...");
            try {
                return getCache().find(getPrimaryKeyField(), primaryKeyValue, loader, getPrimaryKeyField(), primaryKeyValue);
            }
            catch( CacheManagementException e ) {
                throw new PersistenceException(e);
            }
            catch( RuntimeException e ) {
                Throwable t = e.getCause();
                
                if( t != null && t instanceof PersistenceException ) {
                    throw (PersistenceException)t;
                }
                if( logger.isDebugEnabled() ) {
                    logger.error(e.getMessage(), e);
                }
                throw new PersistenceException(e);
            }
            finally {
                logger.debug("Executed.");
            }
        }
        finally {
            logger.debug("exit - get(String,Object)");
        }
    }    
    
    /**
     * Loads all elements of this class from the data store. Use this method only when you know
     * exactly what you are doing. Otherwise, you will pull a lot of data.
     * @return all objects from the database
     * @throws PersistenceException an error occurred executing the query
     */
    @Override
    public Collection<T> list() throws PersistenceException {
        logger.debug("enter - list()");
        try {
            return find(null, null, false);
        }
        finally {
            logger.debug("exit - list()");
        }
    }
    
    private Map<String,Object> toParams(SearchTerm ... searchTerms) {
        HashMap<String,Object> params = new HashMap<String,Object>();
        
        if( searchTerms != null ) {
            for( SearchTerm term : searchTerms ) {
                params.put(term.getColumn(), term.getValue());
            }
        }
        return params;
    }
    
    @SuppressWarnings("unchecked")
    private Collection<T> load(Loader loader, JiteratorFilter<T> filter, Map<String,Object> params) throws PersistenceException {
        logger.debug("enter - load(Class,SearchTerm...)");
        try {
            Transaction xaction = Transaction.getInstance(true);
            final Jiterator<T> it = new Jiterator<T>(filter);

            params.put("--key--", getPrimaryKey().getFields()[0]);
            try {
                final Map<String,Object> results;
                
                results = xaction.execute(loader, params, readDataSource);
                xaction.commit();
                Thread t = new Thread() {
                    public void run() {
                        try {
                            for( Map<String,Object> map: (Collection<Map<String,Object>>)results.get(Loader.LISTING) ) {
                                it.push(getCache().find(map));
                            }
                            it.complete();
                        }
                        catch( Exception e ) {
                            it.setLoadException(e);
                        }
                        catch( Throwable t ) {
                            it.setLoadException(new RuntimeException(t));
                        }
                    }
                };
                
                t.setDaemon(true);
                t.setName("Loader");
                t.start();
                return new JitCollection<T>(it, getEntityClassName());
            }
            catch( PersistenceException e ) {
                it.setLoadException(e);
                throw e;
            }
            catch( RuntimeException e ) {
                it.setLoadException(e);
                throw e;
            }
            catch( Throwable t ) {
                RuntimeException e = new RuntimeException(t);
                
                it.setLoadException(e);
                throw e;
            }
            finally {
                xaction.rollback();
            }
        }
        finally {
            logger.debug("exit - load(Class,Map)");
        }
    }
    
    /**
     * Removes the specified item from the system permanently.
     * @param xaction the transaction under which this event is occurring
     * @param item the item to be removed
     * @throws PersistenceException an error occurred talking to the data store or
     * removal of these objects is prohibited
     */
    @Override
    public void remove(Transaction xaction, T item) throws PersistenceException {
        xaction.execute(getDeleter(), getCache().getKeys(item), writeDataSource);
        getCache().release(item);
    }
    
    public void remove(Transaction xaction, SearchTerm ... terms) throws PersistenceException {
        for( T item : find(terms) ) {
            remove(xaction, item);
        }
    }
    
    public String toString() {
        return getCache().toString();
    }
    
    /**
     * Updates the specified object with the data provided in the specified state under
     * the governance of the specified transaction.
     * @param xaction the transaction governing this event
     * @param item the item to be updated
     * @param state the new state for the updated object
     * @throws PersistenceException an error occurred talking to the data store, or
     * updates are not supported
     */
    @Override
    public void update(Transaction xaction, T item, Map<String,Object> state) throws PersistenceException {     
        state.put("--key--", getPrimaryKey().getFields()[0]);
        xaction.execute(getUpdater(), state, writeDataSource);
    }   
    
    public Collection<T> hsFind(String index, String... indexValues) throws PersistenceException {
    	return hsFind(index, Operator.EQUALS, indexValues);
    }
    
    public Collection<T> hsFind(String index, Operator operator, String... indexValues) throws PersistenceException {
    	return hsFind(index, Operator.EQUALS, 1, 0, indexValues);    	
    }
    
    /**
     * Hits the HandlerSocket index directly, caching objects it finds.
     * 
     * @param index One of the table's indexes to hit. 
     * @param operator What type of comparison the query will make.
     * @param limit The number of results to return.
     * @param offset Where to start to return the results.
     * @param indexValues The values of the index to find values for.  MAY NOT BE EMPTY!
     * @return The found objects.
     * @throws PersistenceException Thrown if there's an issue.
     */
    public Collection<T> hsFind(final String index, final Operator operator, final int limit, final int offset, final String... indexValues) throws PersistenceException {
    	
    	if (indexValues == null || indexValues.length == 0) {
    		throw new PersistenceException("You must specify index values to use in the lookup!");
    	}
    	
    	FindOperator findOperator;
    	Collection<T> results = new ArrayList<T>();
		ResultSet rs = null;
    			    	
    	switch (operator) {
	    	case EQUALS: findOperator = FindOperator.EQ; break;
			case GREATER_THAN: findOperator = FindOperator.GT; break;
			case GREATER_THAN_OR_EQUAL_TO: findOperator = FindOperator.GE; break;
			case LESS_THAN: findOperator = FindOperator.LT; break;
			case LESS_THAN_OR_EQUAL_TO: findOperator = FindOperator.LE; break;
			default: throw new PersistenceException("Operator " + operator + " not supported!");
    	}
    	
    	try {
    		IndexSession session = getSession(index, databaseColumns);
    		
    		rs = session.find(indexValues, findOperator, limit, offset);
			
			while (rs != null && rs.next()) {
				
				HashMap<String,Object> state = new HashMap<String,Object>();
			    
			    for( int i=1; i<=columns.length; i++) {
			        Object ob = getValue(columns[i-1], i, rs);
			        state.put(columns[i-1], ob);
			    }
			    results.add(getCache().find(state));			    
			}
    	} catch (Exception e) {
    		logger.warn(e.getMessage(), e);
			throw new PersistenceException(e.getMessage());
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				throw new PersistenceException(e.getMessage());
			}
		}
		return results;
    	
    	// COMMENTED OUT BELOW AS NOT ENTIRELY SURE IT WORKS
//    	PopulatorThread<T> populator;
//
//		populator = new PopulatorThread<T>(new JiteratorPopulator<T>() {
//			public void populate(Jiterator<T> iterator) throws PersistenceException {
//				FindOperator findOperator;
//				ResultSet rs = null;
//		    			    	
//		    	switch (operator) {
//			    	case EQUALS: findOperator = FindOperator.EQ; break;
//					case GREATER_THAN: findOperator = FindOperator.GT; break;
//					case GREATER_THAN_OR_EQUAL_TO: findOperator = FindOperator.GE; break;
//					case LESS_THAN: findOperator = FindOperator.LT; break;
//					case LESS_THAN_OR_EQUAL_TO: findOperator = FindOperator.LE; break;
//					default: throw new PersistenceException("Operator " + operator + " not supported!");
//		    	}
//		    	
//		    	try {
//		    		logger.info("Executing find against " + index + " "  + findOperator + " "  + Arrays.toString(indexValues));
//		    		
//		    		IndexSession session = getSession(index, databaseColumns);
//		    		logger.info("Session " + session.getIndexId());
//		    		
//		    		rs = session.find(indexValues, findOperator, limit, offset);
//					
//					logger.info("Success? " + (rs != null));
//					
//					while (rs != null && rs.next()) {
//						
//						HashMap<String,Object> state = new HashMap<String,Object>();
//					    
//					    for( int i=1; i<=columns.length; i++) {
//					        Object ob = getValue(columns[i-1], i, rs);
//					        state.put(columns[i-1], ob);
//					    }
//					    logger.info("Found: " + state);
//					    T item = getCache().find(state);
//					    logger.info("Cached: " + item);
//					    iterator.push(item);
//					}
//		    	} catch (Exception e) {
//		    		logger.error(e.getMessage(), e);
//					throw new PersistenceException(e.getMessage());
//				} finally {
//					try {
//						if (rs != null) {
//							rs.close();
//						}
//					} catch (SQLException e) {
//						throw new PersistenceException(e.getMessage());
//					}
//				}
//			}
//		});
//		populator.populate();
//		return populator.getResult();
    }
    
    /**
	 * Opens a connection and a session to a HandlerSocket server.  Specify the table's index to 
	 * hit as well as the columns you want.  A common index is PRIMRY - for the primary
	 * key.  You may also have a multi-column key named NAME_EMAIL with columns (`name`,`email`).
	 * 
	 */
	private IndexSession getSession(String index, String... columns) throws PersistenceException {
		IndexSession session = null;
		String sessionKey = index + Arrays.hashCode(columns); // both index and columns play a role in opening a session
		
		try {
			if (hsClient == null) {
				hsClient = new HSClientImpl(handlerSocketHost, port, poolSize);
			}
			session = indexSessions.get(sessionKey);
			
			if (session == null) {
				logger.info("Opening session: " + database + " " + getSqlNameForClassName(getEntityClassName()) + " " + index + " " + Arrays.toString(columns));
				session = hsClient.openIndexSession(database, getSqlNameForClassName(getEntityClassName()),
						index, columns);
				
				indexSessions.put(sessionKey, session);
			}
			return session;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new PersistenceException(e);
		} 
	}
	
	protected String getSqlNameForClassName(String cname) {
        String[] parts = cname.split("\\.");
        int i;
        
        if( parts != null && parts.length > 1 ) { 
            cname = parts[parts.length-1];
        }
        i = cname.lastIndexOf("$");
        if( i != -1 ) {
            cname = cname.substring(i+1);
        }                
        return getSqlName(cname);
    }
    
    protected String getSqlName(String nom) {
        StringBuilder sql = new StringBuilder();
        
        for(int i=0; i<nom.length(); i++) {
            char c = nom.charAt(i);
            
            if( Character.isLetter(c) && !Character.isLowerCase(c) ) {
                if( i != 0 ) {
                    sql.append("_");
                }
                sql.append(Character.toLowerCase(c));
            }
            else {
                sql.append(c);
            }
        }
        return sql.toString();
    }
    
    protected Map<String,Class<?>> getTypes() {
        return types;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object getValue(String col, int i, ResultSet rs) throws SQLException {
        Class<?> type = getTypes().get(col);
        Object ob;
        
        if( type.equals(String.class) ) {
            String str = rs.getString(i);
            
            if( rs.wasNull() || str == null ) {
                ob = null;
            }
            else {
                ob = str.trim();
            }
        }
        else if( type.equals(Boolean.class) ) {
            String str = rs.getString(i);
            
            if( rs.wasNull() || str == null ) {
                ob = false;
            }
            else {
                ob = str.equalsIgnoreCase("Y");
            }
        }
        else if( type.equals(Locale.class) ) {
            String str = rs.getString(i);
            
            if( rs.wasNull() || str == null ) {
                ob = null;
            }
            else {
                String[] parts = str.split("_");
                
                if( parts != null && parts.length > 1 ) {
                    ob = new Locale(parts[0], parts[1]);
                }
                else {
                    ob = new Locale(parts[0]);
                }
            }
        }
        else if( type.equals(LocalizationGroup.class) ) {
            String str = rs.getString(i);
            
            if( rs.wasNull() || str == null ) {
                ob = null;
            }
            else {
                ob = LocalizationGroup.valueOf(str);
            }
        }
        else if( Number.class.isAssignableFrom(type) ) {
            if( type.getName().equals(Long.class.getName()) ) {
                long l = rs.getLong(i);
                
                if( rs.wasNull() ) {
                    ob = null;
                }
                else {
                    ob = l;
                }
            }
            else if( type.getName().equals(Integer.class.getName()) || type.getName().equals(Short.class.getName()) ) {
                int x = rs.getInt(i);
                
                if( rs.wasNull() ) {
                    ob = null;
                }
                else {
                    ob = x;
                }
            }
            else if( type.getName().equals(Double.class.getName()) ) {
                double x = rs.getDouble(i);

                if( rs.wasNull() ) {
                    ob = null;
                }
                else {
                    ob = x;
                }
            }
            else if( type.getName().equals(Float.class.getName()) ) {
                float f = rs.getFloat(i);
                
                if( rs.wasNull() ) {
                    ob = null;
                }
                else {
                    ob = f;
                }
            }
            else {
                ob = rs.getBigDecimal(i);
                if( rs.wasNull() ) {
                    ob = null;
                }
            }
        }
        else if( Enum.class.isAssignableFrom(type) ) {
            String str = rs.getString(i);
            
            if( str == null || rs.wasNull() ) {
                ob = null;
            }
            else {
                ob = Enum.valueOf((Class<? extends Enum>)type, str);
            }
        }
        else if( type.equals(UUID.class) ) {
            String str = rs.getString(i);
            
            if( rs.wasNull() || str == null ) {
                ob = null;
            }
            else {
                ob = UUID.fromString(str);
            }
        }
        else if( type.getName().startsWith("java.") ){
            ob = rs.getObject(i);
            if( rs.wasNull() ) {
                ob = null;
            }
        }
        else {
            String str = rs.getString(i);

            if( str == null || rs.wasNull() ) {
                ob = null;
            }
            else {
                try {
                    Method m = type.getDeclaredMethod("valueOf", String.class);
                    
                    ob = m.invoke(null, str);
                }
                catch( Exception e ) {
                    throw new SQLException("I have no idea how to map to " + type.getName());
                }
            }
        }
        return ob;
    }

    @Override
    public String getSchema() throws PersistenceException {
        StringBuilder schema = new StringBuilder();

        schema.append("CREATE TABLE ");
        schema.append(" (");

        schema.append(");");
        return schema.toString();
    }
}

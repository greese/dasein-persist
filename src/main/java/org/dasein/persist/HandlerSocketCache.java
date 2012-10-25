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
import org.dasein.persist.l10n.LocalizationGroup;
import org.dasein.util.CachedItem;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorFilter;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.Translator;

import com.google.code.hs4j.FindOperator;
import com.google.code.hs4j.HSClient;
import com.google.code.hs4j.IndexSession;
import com.google.code.hs4j.impl.HSClientImpl;

/**
 * <p>Implements HanderSocket.  You must use indexes (like PRIMARY) to access data.</p>
 * 
 * <p>TODO: Handle translation field types!</p>
 * 
 * <p>When you want to do additional development on this class, hs4j isn't yet in Sonatype - follow these steps to install it locally:</p>
 * <ul>
 *   <li>Make sure Eclipse is turned off (or have to refresh local repository)
 *   <li>Download hs4j source from https://github.com/killme2008/hs4j</li>
 *   <li>Change to directory and run: mvn install -Dmaven.test.skip=true</li>
 * </ul>
 * 
 * @author morgan
 *
 * @see {@link HandlerSocketCache.getSession()}
 * @see https://github.com/ahiguti/HandlerSocket-Plugin-for-MySQL/blob/master/docs-en/protocol.en.txt
 * @see http://code.google.com/p/hs4j/wiki/GettingStarted
 */
public class HandlerSocketCache<T extends CachedItem> extends PersistentCache<T> {
	static public final Logger logger = Logger.getLogger(HandlerSocketCache.class);
	
	static private String database;
	static private String handlerSocketHost;
	static private int port;
	static private int poolSize;
	
	static private HSClient hsClient;	
	static private Map<String,IndexSession> indexSessions;
	static private String[] columns;
	
	private HashMap<String,Class<?>> types = new HashMap<String,Class<?>>();
	
	static {
        Properties props = new Properties();
        
        try {
            InputStream is = DaseinSequencer.class.getResourceAsStream(DaseinSequencer.PROPERTIES);

            if( is != null ) {
                props.load(is);
            }
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        database = props.getProperty("dasein.persist.handlersocket.database");
        handlerSocketHost = props.getProperty("dasein.persist.handlersocket.host");
        port = Integer.valueOf(props.getProperty("dasein.persist.handlersocket.port"));
        poolSize = Integer.valueOf(props.getProperty("dasein.persist.handlersocket.poolSize"));
    }
	
	public HandlerSocketCache() {}
	
	/**
     * Constructs a new persistent factory for objects of the specified class with 
     * the named unique identifier attributes.
     * @param cls the class of objects managed by this factory
     * @param keys a list of unique identifiers for instances of the specified class
     */
    protected void init(Class<T> cls, Key ... keys) {
    	
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
        
        for (int i = 0; i < targetColumns.size(); i++) {
        	columns[i] = targetColumns.get(i);
        }        
    }
	
	@Override
	public T create(Transaction xaction, Map<String, Object> state)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Very simply this the specified index by term.getColumn() using
	 * term.getValue().  THIS DOES NOT USE COLUMNS IN ITS SEARCH, IT USES
	 * INDEXES!!!!!!!!!  
	 */
	@Override
	public Collection<T> find(final SearchTerm[] terms, JiteratorFilter<T> filter,
			Boolean orderDesc, String... orderFields)
			throws PersistenceException {

		PopulatorThread<T> populator;

		populator = new PopulatorThread<T>(new JiteratorPopulator<T>() {
			public void populate(Jiterator<T> iterator) throws PersistenceException {
				ResultSet rs = null;
				try {             
					String index = null;
					String[] values = new String[terms.length];
					FindOperator operator = null;

					for (int i = 0; i < terms.length; i++) {
						// since no other way to get index, just assume column
						index = terms[i].getColumn(); 
						values[i] = terms[i].getValue().toString();		
						
						switch (terms[i].getOperator()) {
							case EQUALS: operator = FindOperator.EQ; break;
							case GREATER_THAN: operator = FindOperator.GT; break;
							case GREATER_THAN_OR_EQUAL_TO: operator = FindOperator.GE; break;
							case LESS_THAN: operator = FindOperator.LT; break;
							case LESS_THAN_OR_EQUAL_TO: operator = FindOperator.LE; break;
							default: throw new PersistenceException("Operator " + operator + " not supported!");							
						}						
					}

					// open a session to retrieve the primary key of the table by the values of the specified index
					rs = getSession(index, getPrimaryKeyField()).find(values, operator, 1, 0);

					// for each row, get the object from cache
					while (rs.next()) {
						T object = get(getValue(getPrimaryKeyField(), 1, rs));
						if (object != null) {
							iterator.push(object);
						}
					}
				} catch (Exception e) {
					throw new PersistenceException(e.getMessage());
				} finally {
					try {
						rs.close();
					} catch (SQLException e) {
						throw new PersistenceException(e.getMessage());
					}
				}
			}
		});
		populator.populate();
		return populator.getResult();
	}

	@Override
	public T get(Object valueOfPrimaryKey) throws PersistenceException {
		T item = getCache().find(getPrimaryKeyField(), valueOfPrimaryKey.toString());
        
        if( item == null ) {
        	ResultSet rs = null;
        	
        	try {
        		// open a session on PRIMARY but retrieve all columns!
				rs = getSession("PRIMARY", columns).find(new String[] {valueOfPrimaryKey.toString()});
				if (rs.next()) {
					HashMap<String,Object> state = new HashMap<String,Object>();
				    
				    for( int i=1; i<=columns.length; i++) {
				        Object ob = getValue(columns[i-1], i, rs);
				        // leave ob there - may use for debugging in the future
				        state.put(columns[i-1], ob);
				    }
				    
				    item = getCache().find(state);
				}
				
			} catch (Exception e) {
				throw new PersistenceException(e);
			} finally {
				try {
					rs.close();
				} catch (SQLException e) {
					throw new PersistenceException(e);
				}
			}
        }
        
        return item;
	}

	@Override
	public Collection<T> list() throws PersistenceException {
		//TODO not implemented b/c HandlerSocket doesn't support getting records w/o lookup value
		
		throw new PersistenceException("Not implemented!");
//		PopulatorThread<T> populator;
//        
//        populator = new PopulatorThread<T>(new JiteratorPopulator<T>() {
//            public void populate(Jiterator<T> iterator) throws PersistenceException {
//            	ResultSet rs = null;
//                try {                	
//                	// open a session to hit only the PRIMARY index and field
//                	rs = getSession("PRIMARY", getPrimaryKeyField()).find(null);
//                	
//                	while (rs.next()) {
//                		T object = get(rs.getObject(getPrimaryKeyField()));
//                		if (object != null) {
//                			iterator.push(object);
//                		}
//                	}
//                } catch (Exception e) {
//                	throw new PersistenceException(e.getMessage());
//                } finally {
//                	try {
//						rs.close();
//					} catch (SQLException e) {
//						throw new PersistenceException(e.getMessage());
//					}
//                }
//                
//            }
//        });
//        populator.populate();
//        return populator.getResult();
	}

	@Override
	public void remove(Transaction xaction, T item) throws PersistenceException {
		// TODO implement
		
	}
	
    public void remove(Transaction xaction, SearchTerm ... terms) throws PersistenceException {
        for( T item : find(terms) ) {
            remove(xaction, item);
        }
    }

	@Override
	public void update(Transaction xaction, T item, Map<String, Object> state)
			throws PersistenceException {
		// TODO implement
		
	}

    @Override
    public String getSchema() throws PersistenceException {
        StringBuilder schema = new StringBuilder();

        schema.append("CREATE TABLE ");
        schema.append(getSqlNameForClassName(getEntityClassName()));
        schema.append(" (");

        schema.append(");");
        return schema.toString();
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
				session = hsClient.openIndexSession(database, this.getSqlNameForClassName(getEntityClassName()),
						index, columns);
				
				indexSessions.put(sessionKey, session);
			}
			return session;
		} catch (Exception e) {
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
    
}
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

/* $Id: Loader.java,v 1.16 2009/02/21 19:44:38 greese Exp $ */
/* Copyright (c) 2006 Valtira Corporation, All Rights Reserved */
package org.dasein.persist.jdbc;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.PersistentCache.EntityJoin;
import org.dasein.persist.Transaction;
import org.dasein.persist.l10n.LocalizationGroup;
import org.dasein.util.CachedItem;
import org.dasein.util.uom.Measured;
import org.dasein.util.uom.UnitOfMeasure;

public class Loader extends AutomatedSql {
    static public final Logger logger = Logger.getLogger(Loader.class);
    
    static public final String LISTING = "listing";
    
    private boolean                                     descending;
    private ArrayList<String>                           order;
    private String                                      sql;
    
    public Loader() {
        super();
    }
    
    public synchronized String getStatement() throws SQLException {
        logger.debug("enter - getStatement()");
        try {
            if( sql == null ) {
                StringBuilder str = new StringBuilder();
                Iterator<String> it = getColumns().iterator();
    
                str.append("SELECT ");
                while( it.hasNext() ) {
                    String col = it.next();
                    
                    str.append(getIdentifier(getTableName(), getSqlName(col)));
                    if( it.hasNext() ) {
                        str.append(", ");
                    }
                }
                str.append(" FROM ");
                str.append(getIdentifier(getTableName()));
                if( !getCriteria().isEmpty() ) {
                    ArrayList<Class<? extends CachedItem>> joins = new ArrayList<Class<? extends CachedItem>>();
                    Iterator<Criterion> criteria;

                    criteria = getCriteria().iterator();
                    while( criteria.hasNext() ) {
                        Criterion c = criteria.next();
                        
                        if( c.entity != null && !joins.contains(c.entity) ) {
                            EntityJoin j = getEntityJoin(c.entity);
                                
                            if( j != null ) { 
                                joins.add(c.entity);
                            }
                        }
                    }
                    if( joins.size() > 0 ) {
                        for( Class<? extends CachedItem> c : joins ) {
                            str.append(",");
                            str.append(getIdentifier(getSqlName(c)));
                        }
                        str.append(" WHERE ");
                        for( Class<? extends CachedItem> c : joins ) {
                            EntityJoin j = getEntityJoin(c);
                            
                            if( j != null ) {
                                str.append(getIdentifier(getTableName(), getSqlName(j.localField)));
                                str.append("=");
                                str.append(getIdentifier(getSqlName(j.joinEntity), getSqlName(j.joinField)));
                                str.append(" AND ");
                            }
                        }
                    }
                    else {
                        str.append(" WHERE ");
                    }
                    criteria = getCriteria().iterator();
                    while( criteria.hasNext() ) {
                        Criterion criterion = criteria.next();

                        if( criterion.entity == null ) {
                            str.append(getIdentifier(getTableName(), getSqlName(criterion.column)));
                        }
                        else {
                            str.append(getIdentifier(getSqlName(criterion.entity), getSqlName(criterion.column)));
                        }
                        str.append(" ");
                        str.append(criterion.operator.toString());
                        str.append(" ?");
                        if( criteria.hasNext() ) {
                            str.append(" ");
                            str.append(getJoin().toString());
                            str.append(" ");
                        }
                    }
                }
                if( order != null && order.size() > 0 ) {
                    str.append(" ORDER BY ");
                    it = order.iterator();
                    while( it.hasNext() ) {
                        String col = it.next();
                        
                        str.append(getIdentifier(getTableName(), getSqlName(col)));
                        if( it.hasNext() ) {
                            str.append(", ");
                        }
                    }
                    if( descending ) {
                        str.append(" DESC ");
                    }
                }
                sql = str.toString();
            }
            return sql;
        }
        finally {
            logger.debug("exit - getStatement()");
        }
    }
    
    protected void setOrder(boolean desc, String... cols) {
        if( order == null ) {
            order = new ArrayList<String>();
        }
        descending = desc;
        if( cols != null ) {
            for( String col : cols ) {
                order.add(col);
            }
        }
    }
    
    public void prepare(Map<String,Object> params) throws SQLException {
        int i = 1;
        
        for( Criterion criterion : getCriteria() ) {
            prepare(criterion.column, i++, params.get(criterion.column));
        }
    }
    
    public Map<String,Object> run(Transaction xaction, Map<String,Object> params) throws SQLException, PersistenceException {
        ArrayList<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        HashMap<String,Object> map = new HashMap<String,Object>();
        int count = getColumns().size();
        long startTimestamp = System.currentTimeMillis();
        
        map.put(LISTING, list);
        prepare(params);
        ResultSet results = statement.executeQuery();
        long queryStopTimestamp = System.currentTimeMillis();

        try {
            while( results.next() ) {
                HashMap<String,Object> state = new HashMap<String,Object>();
                
                for( int i=1; i<=count; i++) {
                    Object ob = getValue(getColumns().get(i-1), i, results);
                    
                    state.put(getColumns().get(i-1), ob);
                }
                list.add(state);
            }
        }
        finally {
            try { results.close(); }
            catch( SQLException e ) {
                logger.error("Problem closing results: " + e.getMessage(), e);
            }
        }

        long endTimestamp = System.currentTimeMillis();

        if( (endTimestamp - startTimestamp) > (2000L) ) {
            String queryTime = Long.toString((queryStopTimestamp - startTimestamp));
            String totalRsTime = Long.toString((endTimestamp - queryStopTimestamp));

            String debugTiming = "[query: "+ queryTime + ",rs: " + totalRsTime+"]";

            logger.warn("SLOW QUERY: " + sql + " "+ debugTiming);
        }
        if( isTranslating() ) {
            for( Map<String,Object> item : list ) {
                Object key = item.get((String)params.get("--key--"));
                
                item.putAll(loadStringTranslations(xaction, getTarget(), key.toString()));
            }            
        }
        return map;
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
        else if( type.equals(Boolean.class) || type.equals(boolean.class)) {
            String str = rs.getString(i);
            
            ob = (!rs.wasNull() && str != null && str.equalsIgnoreCase("Y"));
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
        else if( Measured.class.isAssignableFrom(type) ) {
            ParameterizedType pt = getParameterizedTypes().get(col);
            Number num;
            
            try {
                num = loadNumber(double.class, rs, i);
                if( num == null ) {
                    ob = null;
                }
                else {
                    Constructor<? extends Measured> constructor = null;
                    
                    for( Constructor<?> c : type.getDeclaredConstructors() ) {
                        Class[] args = c.getParameterTypes();
                        
                        if( args != null && args.length == 2 && Number.class.isAssignableFrom(args[0]) && UnitOfMeasure.class.isAssignableFrom(args[1]) ) {
                            constructor = (Constructor<? extends Measured>)c;
                            break;
                        }
                    }
                    if( constructor == null ) {
                        throw new SQLException("Unable to map with no proper constructor");
                    }
                    else {
                        ob = constructor.newInstance(num, ((Class<?>)pt.getActualTypeArguments()[0]).newInstance());
                    }
                }
            }
            catch( SQLException e ) {
                try {
                    Method m = type.getDeclaredMethod("valueOf", String.class);
                    String str = rs.getString(i);
                    
                    if( rs.wasNull() ) {
                        ob = null;
                    }
                    else {
                        ob = m.invoke(null, str);
                    }
                }
                catch( Exception more ) {
                    String err = "I have no idea how to map to " + type.getName() + " / " + pt + ": " + e.getMessage();
                    logger.error(err, e);
                    throw new SQLException(err);
                }
            }
            catch( Exception e ) {
                String err = "Unable to load data for type " + type + " - " + pt + ": " + e.getMessage();
                logger.error(err, e);
                throw new SQLException(err);
            }
        }
        else if( Number.class.isAssignableFrom(type) || type.equals(long.class) || type.equals(int.class) || type.equals(short.class) || type.equals(float.class) || type.equals(double.class) ) {
            ob = loadNumber(type, rs, i);
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
    
    private Number loadNumber(Class<?> type, ResultSet rs, int i) throws SQLException {
        if( type.getName().equals(Long.class.getName()) || type.equals(long.class) ) {
            long l = rs.getLong(i);
            
            if( rs.wasNull() ) {
                return null;
            }
            else {
                return l;
            }
        }
        else if( type.getName().equals(Integer.class.getName()) || type.getName().equals(Short.class.getName()) || type.equals(int.class) || type.equals(short.class) ) {
            int x = rs.getInt(i);
            
            if( rs.wasNull() ) {
                return null;
            }
            else {
                return x;
            }
        }
        else if( type.getName().equals(Double.class.getName()) || type.equals(double.class) ) {
            double x = rs.getDouble(i);

            if( rs.wasNull() ) {
                return null;
            }
            else {
                return x;
            }
        }
        else if( type.getName().equals(Float.class.getName()) || type.equals(float.class) ) {
            float f = rs.getFloat(i);
            
            if( rs.wasNull() ) {
                return null;
            }
            else {
                return f;
            }
        }
        return rs.getBigDecimal(i);
    }
}

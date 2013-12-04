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

/* $Id: AutomatedSql.java,v 1.25 2009/07/02 01:36:20 greese Exp $ */
/* Copyright (c) 2006 Valtira Corporation, All Rights Reserved */
package org.dasein.persist.jdbc;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.PersistentFactory;
import org.dasein.persist.Transaction;
import org.dasein.persist.PersistentCache.EntityJoin;
import org.dasein.persist.l10n.LocalizationGroup;
import org.dasein.util.CachedItem;
import org.dasein.util.Translator;
import org.dasein.util.uom.Measured;

public class AutomatedSql extends Execution {
    static public enum Operator {
        EQUALS, LIKE, NOT_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO, NULL, NOT_NULL;

        public String toString() {
            switch( this ) {
            case EQUALS: return "=";
            case LIKE: return "LIKE";
            case NOT_EQUAL: return "<>";
            case GREATER_THAN: return ">";
            case GREATER_THAN_OR_EQUAL_TO: return ">=";
            case LESS_THAN: return "<";
            case LESS_THAN_OR_EQUAL_TO: return "<=";
            case NULL: return "IS";
            case NOT_NULL: return "IS NOT";
            default: return "=";
            }
        }
    }

    static public enum Join {
        OR, AND
    }

    static public class Criterion {
        public String                        column   = null;
        public Class<? extends CachedItem>   entity   = null;

        public Operator operator = Operator.EQUALS;

        public Criterion(String col) {
            this(null, col, Operator.EQUALS);
        }

        public Criterion(String col, Operator oper) {
            this(null, col, oper);
        }

        public Criterion(Class<? extends CachedItem> entity, String column, Operator operator) {
            super();
            this.entity = entity;
            this.column = column;
            this.operator = operator;
        }
    }

    static public enum TranslationMethod { NONE, STANDARD, CUSTOM };

    static private final Logger logger = Logger.getLogger(AutomatedSql.class);

    private ArrayList<String>        columns           = new ArrayList<String>();
    private ArrayList<Criterion>     criteria          = new ArrayList<Criterion>();
    private Map<Class<? extends CachedItem>,EntityJoin> entityJoins = null;
    private Join                     join              = Join.AND;
    private String                   table             = null;
    private Class<?>                 target            = null;
    private TranslationMethod        translationMethod = TranslationMethod.NONE;
    private Collection<String>       translators       = new ArrayList<String>();
    private HashMap<String,Class<?>> types             = new HashMap<String,Class<?>>();
    private HashMap<String,ParameterizedType> ptypes             = new HashMap<String,ParameterizedType>();
    
    public AutomatedSql() {
        init();
    }

    protected void init() {
        // NO-OP
    }

    protected List<String> getColumns() {
        return columns;
    }

    protected List<Criterion> getCriteria() {
        return criteria;
    }

    protected EntityJoin getEntityJoin(Class<? extends CachedItem> forClass) {
        return (entityJoins == null ? null : entityJoins.get(forClass));
    }

    protected Collection<Class<? extends CachedItem>> getJoinEntities() {
        if( entityJoins == null ) {
            return Collections.emptyList();
        }
        else {
            return entityJoins.keySet();
        }
    }

    public Join getJoin() {
        return join;
    }

    protected String getJoinTable(Class<?> c1, Class<?> c2) {
        String cname1 = getSqlName(c1);
        String cname2 = getSqlName(c2);
        int x = cname1.compareTo(cname2);

        if( x < 0 ) {
            return cname1 + "_" + cname2;
        }
        else {
            return cname2 + "_" + cname1;
        }
    }

    protected Map<String,ParameterizedType> getParameterizedTypes() {
        return ptypes;
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

    protected String getSqlName(Class<?> cls) {
        return getSqlNameForClassName(cls.getName());
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

    protected String getTableName() {
        return table;
    }

    public Class<?> getTarget() {
        return target;
    }
    
    public TranslationMethod getTranslationMethod() {
        return translationMethod;
    }

    protected Collection<String> getTranslators() {
        return translators;
    }

    protected Map<String,Class<?>> getTypes() {
        return types;
    }

    public boolean isTranslating() {
        return !translationMethod.equals(TranslationMethod.NONE);
    }

    protected void parseFields(Class<?> cls, Collection<String> cols, Map<String,Class<?>> t, Map<String,ParameterizedType> pt, Collection<String> trans) {
        while( cls != null && !cls.equals(Object.class) ) {
            Field[] fields = cls.getDeclaredFields();

            for( Field f : fields ) {
                int m = f.getModifiers();

                if( Modifier.isTransient(m) || Modifier.isStatic(m) ) {
                    continue;
                }
                if (f.getType().getName().equals(Collection.class.getName())) {
                    continue; // this is handled by dependency manager
                }
                if( !f.getType().getName().equals(Translator.class.getName()) ) {
                    cols.add(f.getName());
                    t.put(f.getName(), f.getType());
                    if( Measured.class.isAssignableFrom(f.getType()) ) {
                        pt.put(f.getName(), (ParameterizedType)f.getGenericType());
                    }
                }
                else {
                    trans.add(f.getName());
                }
            }
            cls = cls.getSuperclass();
        }
    }

    public void prepare(String col, int i, Object ob) throws SQLException {
        Class<?> t;

        if( logger.isDebugEnabled() ) {
            logger.debug("Preparing " + col + " (" + i + "): " + ob);
        }
        if( col.equals("timestamp") ) {
            Class<?> tmp = getTypes().get("lastModified");

            if( tmp != null ) {
                t = tmp;
            }
            else {
                t = getTypes().get(col);
            }
        }
        else {
            t = getTypes().get(col);
        }
        if( t == null ) {
            for( Criterion criterion : getCriteria() ) {
                if( criterion.entity != null && criterion.column.equals(col) ) {
                    Class<? extends CachedItem> cls = criterion.entity;

                    while( cls != null && !cls.equals(Object.class) ) {
                        Field[] fields = cls.getDeclaredFields();

                        for( Field f : fields ) {
                            if( f.getName().equals(criterion.column) ) {
                                int m = f.getModifiers();

                                if( Modifier.isTransient(m) || Modifier.isStatic(m) ) {
                                    continue;
                                }
                                if (f.getType().getName().equals(Collection.class.getName()) ) {
                                    continue; // this is handled by dependency manager
                                }
                                if( !f.getType().getName().equals(Translator.class.getName()) ) {
                                    t = f.getType();
                                    break;
                                }
                            }
                        }
                        if( t != null ) {
                            break;
                        }
                    }
                }
                if( t != null ) {
                    break;
                }
            }
        }
        prepare(col, i, ob, t);
    }

    public void prepare(String col, int i, Object ob, Class<?> t) throws SQLException {
        try {
            if( t.equals(Boolean.class) || t.equals(boolean.class) ) {
                boolean b;

                if( ob == null ) {
                    b = false;
                }
                else {
                    b = (Boolean)ob;
                }
                statement.setString(i, b ? "Y" : "N");
            }
            else if( t.equals(String.class) ) {
                if( ob == null ) {
                    statement.setNull(i, Types.VARCHAR);
                }
                else {
                    statement.setString(i, (String)ob);
                }
            }
            else if( t.equals(Locale.class) ) {
                if( ob == null ) {
                    statement.setNull(i, Types.VARCHAR);
                }
                else {
                    statement.setString(i, ob.toString());
                }
            }
            else if( Measured.class.isAssignableFrom(t) ) {
                // TODO: this only works for concrete types in which the sql table is a number
                // need to support strings with the type modifier
                setNumber(i, (ob == null ? null : ((Measured<?,?>)ob).getQuantity()));
            }
            else if( t.equals(Long.class) || t.equals(long.class) ) {
                if( ob == null ) {
                    statement.setNull(i, Types.BIGINT);
                }
                else if( ob instanceof String ) {
                    statement.setLong(i, Long.parseLong((String)ob)); 
                }
                else {
                    statement.setLong(i, ((Number)ob).longValue());
                }
            }
            else if( t.equals(Integer.class) || t.equals(int.class) || t.equals(Short.class) || t.equals(short.class) ) {
                if( ob == null ) {
                    statement.setNull(i, Types.INTEGER);
                }
                else if( ob instanceof String ) {
                    statement.setInt(i, Integer.parseInt((String)ob));
                }
                else {
                    statement.setInt(i, ((Number)ob).intValue());
                }
            }
            else if( t.equals(Float.class) || t.equals(float.class) ) {
                if( ob == null ) {
                    statement.setNull(i, Types.FLOAT);
                }
                else if( ob instanceof String ) {
                    statement.setFloat(i, Float.parseFloat((String)ob));
                }
                else {
                    statement.setFloat(i, ((Number)ob).floatValue());
                }
            }
            else if( t.equals(Double.class) || t.equals(double.class) ) {
                if( ob == null ) {
                    statement.setNull(i, Types.DOUBLE);
                }
                else if( ob instanceof String ) {
                    statement.setDouble(i, Double.parseDouble((String)ob));
                }
                else {
                    statement.setDouble(i, ((Number)ob).doubleValue());
                }
            }
            else if( Number.class.isAssignableFrom(t) ) {
                if( ob instanceof String ) {
                    setNumber(i, Double.parseDouble((String)ob));
                }
                else {
                    setNumber(i, (Number)ob);
                }
            }
            else if( Enum.class.isAssignableFrom(t) ) {
                if( ob == null ) {
                    statement.setNull(i, Types.VARCHAR);
                }
                else {
                    statement.setString(i, ((Enum)ob).name());
                }
            }
            else if( t.equals(LocalizationGroup.class) ) {
                if( ob == null ) {
                    statement.setNull(i, Types.VARCHAR);
                }
                else {
                    statement.setString(i, ((LocalizationGroup)ob).getLocalizationCode());
                }
            }
            else if( t.getName().startsWith("java.") ) {
                if(ob == null) {
                    statement.setNull(i, Types.BLOB);
                }
                else {
                    statement.setObject(i, ob);
                }
            }
            else if( t.equals(UUID.class) ) {
                if( ob == null ) {
                    statement.setNull(i, Types.VARCHAR);
                }
                else {
                    statement.setString(i, ob.toString());
                }
            }
            else {
                if( ob == null ) {
                    statement.setNull(i, Types.VARCHAR);
                }
                else {
                    statement.setString(i, ob.toString());
                }
            }
        }
        catch( ClassCastException e ) {
            String cname, tname;

            if( ob != null ) {
                cname = ob.getClass().getName();
            }
            else {
                cname = "Unknown";
            }
            tname = t.getName();
            throw new SQLException("Attempted to assign an object of type " + cname + " to a field of type " + tname + " for " + col);
        }
    }

    private void setNumber(int i, Number ob) throws SQLException {
        if( ob == null ) {
            statement.setNull(i, Types.BIGINT);
        }
        else if( ob instanceof Double ) {
            statement.setDouble(i, ((Double)ob).doubleValue());
        }
        else if( ob instanceof Float ) {
            statement.setFloat(i, ((Float)ob).floatValue());
        }
        else if( ob instanceof Integer ) {
            statement.setInt(i, ((Number)ob).intValue());
        }
        else if( ob instanceof Short ) {
            statement.setInt(i, ((Number)ob).intValue());
        }
        else {
            statement.setLong(i, ((Number)ob).longValue());
        }        
    }
    
    protected void prepareFor(String col, int i, Object ob, String owner) throws SQLException {
        try {
            prepareFor(col, i, ob, Class.forName(owner));
        }
        catch( Exception e ) {
            logger.error("", e);
            throw new SQLException(e.getMessage());
        }
    }

    public void prepareFor(String col, int i, Object ob, Class<?> owner) throws SQLException {
        Field f = null;

        while( f == null ) {
            try {
                f = owner.getDeclaredField(col);
            }
            catch( SecurityException e ) {
                logger.error("", e);
            }
            catch( NoSuchFieldException e ) {
                // ignore
            }
            if( f == null ) {
                owner = owner.getSuperclass();
            }
            if( owner == null || owner.getName().equals(Object.class.getName()) ) {
                break;
            }
        }
        if( f != null ) {
            prepare(col, i, ob, f.getType());
        }
        else {
            throw new SQLException("No match for " + col + " in " + owner.getName());
        }
    }

    protected void setCriteria(String... params) {
        setCriteria(Join.AND, params);
    }

    protected void setCriteria(Criterion... params) {
        setCriteria(Join.AND, params);
    }

    protected void setCriteria(Join j, String... params) {
        join = j;
        for( String p : params ) {
            criteria.add(new Criterion(p));
        }
    }

    protected void setCriteria(Join j, Criterion... params) {
        join = j;
        for( Criterion p : params ) {
            criteria.add(p);
        }
    }

    public void setCustomTranslating() {
        translationMethod = TranslationMethod.CUSTOM;
    }

    protected void setTarget(String cname) {
        try {
            setTarget(Class.forName(cname));
        }
        catch( Exception e ) {
            logger.error("", e);
        }
    }

    protected void setTarget(Class<?> cls) {
        setTarget(getSqlName(cls), cls);
        setDsn(getDataSourceName(cls.getName()));
    }

    protected void setTarget(String tname, Class<?> cls) {
        target = cls;
        table = tname;
        parseFields(cls, columns, types, ptypes, translators);

    }

    protected void setTranslating(boolean t) {
        translationMethod = (t ? TranslationMethod.STANDARD : TranslationMethod.NONE);
    }

    protected void setTranslating(PersistentFactory<?> f) {
        translationMethod = TranslationMethod.CUSTOM;
    }

    public Map<String,Translator<String>> loadStringTranslations(Transaction xaction, Class cls, String id) throws PersistenceException, SQLException {
        if( translators.size() > 0 ) {
            if( translationMethod.equals(TranslationMethod.STANDARD) ) {
                return super.loadStringTranslations(xaction, cls, id);
            }
            else if( translationMethod.equals(TranslationMethod.CUSTOM) ) {
                return loadCustomTranslations(xaction, id);
            }
        }
        return new HashMap<String,Translator<String>>();
    }

    private transient Class<? extends Execution> xloader = null;

    @SuppressWarnings("unchecked")
    private Map<String,Translator<String>> loadCustomTranslations(Transaction xaction, String id) throws PersistenceException, SQLException {
        logger.debug("enter - loadTranslations(Transaction,String)");
        try {
            Map<String,Object> criteria = new HashMap<String,Object>(1);
            Map<String,Translator<String>> map = null;
            Class<?> cls = getTarget();

            criteria.put("ownerId", id);
            if( xloader == null ) {
                xloader = PersistentFactory.compileTranslator(cls, "Loader");
            }
            criteria = xaction.execute(xloader, criteria, Execution.getDataSourceName(cls.getName()));
            map = new HashMap<String,Translator<String>>(criteria.keySet().size());
            // a retarded side-effect of the lame-ass implementation of generics in Java
            for( String attr : criteria.keySet() ) {
                Object trans = criteria.get(attr);

                map.put(attr, (Translator<String>)trans);
            }
            return map;
        }
        finally {
            logger.debug("exit - loadTranslations(Transaction,String)");
        }
    }

    public void removeStringTranslations(Transaction xaction, Class cls, String id) throws PersistenceException, SQLException {
        if( translationMethod.equals(TranslationMethod.STANDARD) ) {
            super.removeStringTranslations(xaction, cls, id);
        }
        else if( translationMethod.equals(TranslationMethod.CUSTOM) ) {
            removeCustomTranslations(xaction, id);
        }
    }

    private transient Class<? extends Execution> xdeleter = null;

    private void removeCustomTranslations(Transaction xaction, String id) throws PersistenceException, SQLException {
        Map<String,Object> state = new HashMap<String,Object>(1);
        Class<?> cls = getTarget();

        state.put("ownerId", id);
        if( xdeleter == null ) {
            xdeleter = PersistentFactory.compileTranslator(cls, "Deleter");
        }
        xaction.execute(xdeleter, state, Execution.getDataSourceName(cls.getName()));
    }

    public void saveStringTranslation(Transaction xaction, String cname, String id, String attr, Translator<String> t) throws SQLException, PersistenceException {
        if( t == null ) {
            return;
        }
        if( translationMethod.equals(TranslationMethod.STANDARD) ) {
            super.saveStringTranslation(xaction, cname, id, attr, t);
        }
        else if( translationMethod.equals(TranslationMethod.CUSTOM) ) {
            saveCustomTranslations(xaction, id, attr, t);
        }
    }

    private transient Class<? extends Execution> xupdater = null;

    private void saveCustomTranslations(Transaction xaction, String id, String attr, Translator<String> t ) throws PersistenceException, SQLException {
        Map<String,Object> state = new HashMap<String,Object>(3);
        Class cls = getTarget();

        state.put("ownerId", id);
        state.put("attribute", attr);
        state.put("translation", t);
        if( xupdater == null ) {
            xupdater = PersistentFactory.compileTranslator(cls, "Updater");
        }
        xaction.execute(xupdater, state, Execution.getDataSourceName(cls.getName()));
    }

    protected void setEntityJoins(Map<Class<? extends CachedItem>,EntityJoin> joins) {
        entityJoins = joins;
    }
}

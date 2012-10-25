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
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Currency;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.dasein.persist.annotations.AutoJSON;
import org.dasein.persist.annotations.Index;
import org.dasein.persist.annotations.IndexType;
import org.dasein.persist.annotations.Lookup;
import org.dasein.persist.annotations.Schema;
import org.dasein.util.CachedItem;
import org.dasein.util.ConcurrentMultiCache;
import org.dasein.util.CursorPopulator;
import org.dasein.util.ForwardCursor;
import org.dasein.util.JiteratorFilter;
import org.dasein.util.uom.Measured;
import org.dasein.util.uom.UnitOfMeasure;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class PersistentCache<T extends CachedItem> {
    static private final Logger logger = Logger.getLogger(PersistentCache.class);

    static public class EntityJoin {
        public Class<? extends CachedItem> joinEntity;
        public String                      joinField;
        public String                      localField;
    }

    static private final HashMap<String,PersistentCache<? extends CachedItem>> caches = new HashMap<String,PersistentCache<? extends CachedItem>>();

    static public PersistentCache<? extends CachedItem> getCache(Class<? extends CachedItem> forClass) throws PersistenceException {
        SchemaMapper[] mappers = null;
        String schemaVersion = null;
        String entityName = null;

        for( Annotation annotation : forClass.getDeclaredAnnotations() ) {
            if( annotation instanceof Schema ) {
                schemaVersion = ((Schema)annotation).value();
                entityName = ((Schema)annotation).entity();

                Class<? extends SchemaMapper>[] mclasses = ((Schema)annotation).mappers();

                if( mclasses != null && mclasses.length > 0 ) {
                    mappers = new SchemaMapper[mclasses.length];
                    for( int i=0; i<mclasses.length; i++ ) {
                        try {
                            mappers[i] = mclasses[i].newInstance();
                        }
                        catch( Throwable t ) {
                            throw new PersistenceException(t.getMessage());
                        }
                    }
                }
            }
        }
        return getCacheWithSchema(forClass, entityName, schemaVersion == null ? "0" : schemaVersion, mappers);
    }
    
    static public PersistentCache<? extends CachedItem> getCacheWithSchema(@Nonnull Class<? extends CachedItem> forClass, @Nullable String entityName, @Nonnull String schemaVersion, @Nullable SchemaMapper ... mappers) throws PersistenceException {
        Class<?> cls = forClass;

        while( !cls.getName().equals(Object.class.getName()) ) {
            for( Field field : cls.getDeclaredFields() ) {
                for( Annotation annotation : field.getDeclaredAnnotations() ) {
                    if( annotation instanceof Index ) {
                        Index idx = (Index)annotation;

                        if( idx.type().equals(IndexType.PRIMARY) ) {
                            return getCacheWithSchema(forClass, entityName, field.getName(), schemaVersion, mappers);
                        }
                    }
                }
            }
            cls = cls.getSuperclass();
        }
        throw new PersistenceException("No primary key field identified for: " + forClass.getName());
    }

    static public PersistentCache<? extends CachedItem> getCache(Class<? extends CachedItem> forClass, String primaryKey) throws PersistenceException {
        SchemaMapper[] mappers = null;
        String schemaVersion = null;
        String entityName = null;

        for( Annotation annotation : forClass.getDeclaredAnnotations() ) {
            if( annotation instanceof Schema ) {
                schemaVersion = ((Schema)annotation).value();
                entityName = ((Schema)annotation).entity();

                Class<? extends SchemaMapper>[] mclasses = ((Schema)annotation).mappers();

                if( mclasses != null && mclasses.length > 0 ) {
                    mappers = new SchemaMapper[mclasses.length];
                    for( int i=0; i<mclasses.length; i++ ) {
                        try {
                            mappers[i] = mclasses[i].newInstance();
                        }
                        catch( Throwable t ) {
                            throw new PersistenceException(t.getMessage());
                        }
                    }
                }
            }
        }
        return getCacheWithSchema(forClass, entityName, primaryKey, schemaVersion == null ? "0" : schemaVersion, mappers);
    }
    
    @SuppressWarnings("unchecked")
    static public PersistentCache<? extends CachedItem> getCacheWithSchema(@Nonnull Class<? extends CachedItem> forClass, @Nullable String alternateEntytName, @Nonnull String primaryKey, @Nonnull String schemaVersion, @Nullable SchemaMapper ... mappers) throws PersistenceException {
        PersistentCache<? extends CachedItem> cache = null;
        String className = forClass.getName();

        synchronized( caches ) {
            cache = caches.get(className);
            if( cache != null ) {
                return cache;
            }
        }

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
        
        TreeSet<Key> keys = new TreeSet<Key>();
        Class<?> cls = forClass;

        while( !cls.getName().equals(Object.class.getName()) ) {
            for( Field field : cls.getDeclaredFields() ) {
                for( Annotation annotation : field.getDeclaredAnnotations() ) {
                    if( annotation instanceof Index ) {
                        Index idx = (Index)annotation;
                        
                        if( idx.type().equals(IndexType.SECONDARY) || idx.type().equals(IndexType.FOREIGN) ) {
                            String keyName = field.getName();
                            
                            if( idx.multi() != null && idx.multi().length > 0 ) {
                                if( idx.cascade() ) {
                                    int len = idx.multi().length;

                                    keys.add(new Key(keyName));
                                    for( int i=0; i<len; i++ ) {
                                        String[] parts = new String[i+2];
                                        
                                        parts[0] = keyName;
                                        for( int j=0; j<=i; j++ ) {
                                            parts[j+1] = idx.multi()[j];
                                        }
                                        keys.add(new Key(parts));
                                    }
                                }
                                else {
                                    String[] parts = new String[idx.multi().length + 1];
                                    int i = 1;
                                    
                                    parts[0] = keyName;
                                    for( String name : idx.multi() ) {
                                        parts[i++] = name;
                                    }
                                    Key k = new Key(parts);
                                    
                                    keys.add(k);
                                }
                            }
                            else {
                                Key k;
                                
                                if( idx.type().equals(IndexType.FOREIGN) && !idx.identifies().equals(CachedItem.class) ) {
                                    k = new Key(idx.identifies(), keyName);
                                }
                                else {
                                    k = new Key(keyName);
                                }   
                                keys.add(k);
                            }
                        }
                    }
                }
            }
            cls = cls.getSuperclass();
        }
        String propKey = "dsn.persistentCache." + className;
        String prop;

        while( cache == null && !propKey.equals("dsn.persistentCache") ) {
            prop = props.getProperty(propKey);
            if( prop != null ) {
                try {
                    cache = (PersistentCache<? extends CachedItem>)Class.forName(prop).newInstance();
                    cache.initBase(forClass, alternateEntytName, schemaVersion, mappers, new Key(primaryKey), keys.toArray(new Key[keys.size()]));
                    break;
                }
                catch( Throwable t ) {
                    logger.error("Unable to load persistence cache " + prop + ": " + t.getMessage());
                    throw new PersistenceException("Unable to load persistence cache " + prop + ": " + t.getMessage());
                }
            }
            int idx = propKey.lastIndexOf('.');

            propKey = propKey.substring(0,idx);
        }
        if( cache == null ) {
            prop = props.getProperty("dsn.cache.default");
            if( prop == null ) {
                throw new PersistenceException("No persistent cache implementations defined.");
            }
            try {
                cache = (PersistentCache<? extends CachedItem>)Class.forName(prop).newInstance();
                cache.initBase(forClass, alternateEntytName, schemaVersion, mappers, new Key(primaryKey), keys.toArray(new Key[keys.size()]));
            }
            catch( Throwable t ) {
                logger.error("Unable to load persistence cache " + prop + ": " + t.getMessage());
                t.printStackTrace();
                throw new PersistenceException("Unable to load persistence cache " + prop + ": " + t.getMessage());
            }
        }
        synchronized( caches ) {
            PersistentCache<? extends CachedItem> c = caches.get(className);

            if( c != null ) {
                cache = c;
            }
            else {
                caches.put(className, cache);
            }
        }
        return cache;
    }

    private ConcurrentMultiCache<T>                     cache           = null;
    private String                                      entityName      = null;
    private Map<Class<? extends CachedItem>,EntityJoin> joins           = new HashMap<Class<? extends CachedItem>,EntityJoin>();
    private Map<String,LookupDelegate>                  lookups         = new HashMap<String,LookupDelegate>();
    private Key                                         primaryKey      = null;
    private SchemaMapper[]                              schemaMappers   = null;
    private String                                      schemaVersion   = null;
    private Key[]                                       secondaryKeys   = null;

    public PersistentCache() { }

    public String getEntityClassName() {
        return getTarget().getName();
    }

    public @Nullable String getEntityName() {
        return entityName;
    }

    public void addJoinEntity(Class<? extends CachedItem> entity, String joinField, String localField) {
        EntityJoin join = new EntityJoin();

        join.joinEntity = entity;
        join.joinField = joinField;
        join.localField = localField;
        joins.put(entity, join);
    }

    protected EntityJoin getJoin(Class<? extends CachedItem> entity) {
        return joins.get(entity);
    }

    public Map<Class<? extends CachedItem>,EntityJoin> getJoins() {
        return joins;
    }

    protected final void initBase(@Nonnull Class<? extends CachedItem> c, @Nullable String entity, @Nonnull String version, @Nullable SchemaMapper[] mappers, @Nonnull Key primaryKey, @Nullable Key ... keys) {
        @SuppressWarnings("unchecked") Class<T> cls = (Class<T>)c;

        schemaMappers = (mappers == null ? new SchemaMapper[0] : mappers);
        schemaVersion = version;
        entityName = ((entity == null || entity.length() < 1) ? null : entity);
        this.primaryKey = primaryKey;
        if( keys != null && keys.length > 0 ) {
            secondaryKeys = Arrays.copyOf(keys, keys.length);
        }
        else {
            secondaryKeys = new Key[0];
        }
        cache = new ConcurrentMultiCache<T>(cls, primaryKey.getFields()[0]);
        init(cls, keys);
        Class<?> current = cls;
        
        while( !current.getName().equals(Object.class.getName()) ) {
            for( Field field : current.getDeclaredFields() ) {
                for( Annotation annotation : field.getDeclaredAnnotations() ) {
                    if( annotation instanceof Lookup ) {
                        Class<? extends LookupDelegate> delegate = ((Lookup)annotation).delegate();
                        
                        if( delegate != null ) {
                            try {
                                lookups.put(field.getName(), delegate.newInstance());
                            }
                            catch( Throwable t ) {
                                t.printStackTrace();
                            }
                        }
                    }
                }
            }
            current = current.getSuperclass();
        }
    }

    protected void init(Class<T> cls, Key ... keys) {
        // NO-OP
    }

    public long count() throws PersistenceException {
        return list().size();
    }

    public long count(SearchTerm ... terms) throws PersistenceException {
        return find(terms).size();
    }

    protected ConcurrentMultiCache<T> getCache() {
        return cache;
    }

    public abstract T create(Transaction xaction, Map<String,Object> state) throws PersistenceException;

    public Collection<T> find(SearchTerm ... terms) throws PersistenceException {
        return find(terms, null, false);
    }

    public abstract @Nonnull Collection<T> find(@Nonnull SearchTerm[] terms, @Nullable JiteratorFilter<T> filter, @Nullable Boolean orderDesc, @Nullable String ... orderFields) throws PersistenceException;

    public @Nonnull ForwardCursor<T> findAsCursor(@Nonnull SearchTerm[] terms, @Nullable JiteratorFilter<T> filter, @Nullable Boolean orderDesc, @Nullable String ... orderFields) throws PersistenceException {
        final Collection<T> items = find(terms, filter, orderDesc, orderFields);

        CursorPopulator<T> populator = new CursorPopulator<T>(getTarget().getName() + ".findAsCursor", null) {
            @Override
            public void populate(ForwardCursor<T> cursor) {
                for( T item : items ) {
                    cursor.push(item);
                }
            }
        };
        populator.populate();
        return populator.getCursor();
    }

    public abstract T get(Object keyValue) throws PersistenceException;

    protected String getKeyValue(T object) throws PersistenceException {
        return getKeyValue(object, getPrimaryKey());
    }

    @SuppressWarnings("rawtypes")
    protected String getKeyValue(T object, Key key) throws PersistenceException {
        if( object == null ) {
            return null;
        }
        try {
            String value = "";

            for( String fieldName : key.getFields() ) {
                Class<? extends Object> t = getCache().getTarget();
                Field field = null;

                while( field == null ) {
                    field = t.getDeclaredField(fieldName);
                    if( field == null ) {
                        t = t.getSuperclass();
                        if( t.getName().equals(Object.class.getName()) ) {
                            return null;
                        }
                    }
                }
                field.setAccessible(true);
                if( !value.equals("") ) {
                    value = value + ":";
                }
                Object result = field.get(object);

                if( result instanceof Enum ) {
                    value = value + ((Enum)result).name();
                }
                else {
                    value = value + result.toString();
                }
            }
            return value;
        }
        catch( Exception e ) {
            e.printStackTrace();
            throw new PersistenceException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    protected String getKeyValue(Map<String,Object> map, Key key) throws PersistenceException {
        if( map == null ) {
            return null;
        }
        try {
            String value = "";

            for( String fieldName : key.getFields() ) {
                if( !value.equals("") ) {
                    value = value + ":";
                }
                Object result = map.get(fieldName);

                if( result instanceof Enum ) {
                    value = value + ((Enum)result).name();
                }
                else {
                    value = value + result.toString();
                }
            }
            return value;
        }
        catch( Exception e ) {
            e.printStackTrace();
            throw new PersistenceException(e);
        }
    }

    protected Set<String> getKeyValues(int index, SearchTerm ... forTerms) {
        HashSet<String> list = new HashSet<String>();
        SearchTerm term = forTerms[index];
        Object searchValue = term.getValue();
        String keyValue;

        if( searchValue instanceof Range ) {
            keyValue = null;
        }
        else if( searchValue instanceof Enum ) {
            keyValue = ((Enum<?>)searchValue).name();
        }
        else {
            keyValue = searchValue.toString();
        }
        if( index < forTerms.length-1 ) {
            for( String remainder : getKeyValues(index+1, forTerms) ) {
                if( searchValue instanceof Range ) {
                    for( int val : (Range)searchValue ) {
                        list.add(String.valueOf(val) + ":" + remainder);
                    }
                }
                else {
                    list.add(keyValue + ":" + remainder);
                }
            }
        }
        else {
            if( searchValue instanceof Range ) {
                for( int val : (Range)searchValue ) {
                    list.add(String.valueOf(val));
                }
            }
            else {
                list.add(keyValue);
            }
        }
        return list;
    }

    public LookupDelegate getLookupDelegate(String field) {
        return lookups.get(field);
    }
    
    protected Key getPrimaryKey() {
        return primaryKey;
    }

    protected String getPrimaryKeyField() {
        return primaryKey.getFields()[0];
    }

    public long getNewKeyValue() throws PersistenceException {
        return Sequencer.getInstance(getEntityClassName() + "." + getPrimaryKeyField()).next();
    }

    public abstract String getSchema() throws PersistenceException;

    public @Nullable SchemaMapper getSchemaMapper(@Nonnull String fromVersion) {
        if( schemaMappers.length < 1 || fromVersion.equalsIgnoreCase(schemaVersion) ) {
            return null;
        }
        for( SchemaMapper mapper : schemaMappers ) {
            if( mapper.getSourceVersion().equalsIgnoreCase(fromVersion) ) {
                return mapper;
            }
        }
        return null;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    protected Key[] getSecondaryKeys() {
        return secondaryKeys;
    }

    public Class<T> getTarget() {
        return cache.getTarget();
    }

    public Object getValue(T item, String field) {
        Class<?> cls = item.getClass();
        
        while( !cls.equals(Object.class) ) {
            for( Field f : cls.getDeclaredFields() ) {
                if( f.getName().equals(field) ) {
                    f.setAccessible(true);
                    try {
                        return f.get(item);
                    }
                    catch( Exception e ) {
                        throw new RuntimeException(e);
                    }
                }
            }
            cls = cls.getSuperclass();
        }
        return null;
    }
    
    public abstract Collection<T> list() throws PersistenceException;

    public ForwardCursor<T> listAsCursor() throws PersistenceException {
        final Collection<T> items = list();

        CursorPopulator<T> populator = new CursorPopulator<T>(getTarget().getName() + ".listAsCursor", null) {
            @Override
            public void populate(ForwardCursor<T> cursor) {
                for( T item : items ) {
                    cursor.push(item);
                }
            }
        };
        populator.populate();
        return populator.getCursor();
    }

    public Collection<T> list(boolean orderDesc, String ... orderFields) throws PersistenceException {
        if( !orderDesc || (orderFields != null && orderFields.length > 0 ) ) {
            return find(new SearchTerm[0], null, orderDesc, orderFields);
        }
        else {
            return list();
        }
    }

    public ForwardCursor<T> listAsCursor(boolean orderDesc, String ... orderFields) throws PersistenceException {
        if( !orderDesc || (orderFields != null && orderFields.length > 0 ) ) {
            return findAsCursor(new SearchTerm[0], null, orderDesc, orderFields);
        }
        else {
            return listAsCursor();
        }
    }

    protected Key matchKeys(SearchTerm[] terms) {
        if( terms == null ) {
            return null;
        }
        if( terms.length == 1 && terms[0].getColumn().equals(primaryKey.getFields()[0]) ) {
            return primaryKey ;
        }
        for( Key key : secondaryKeys ) {
            if( key.matches(terms) ) {
                return key;
            }
        }
        return null;
    }

    public void reindex() throws PersistenceException {
        // NO-OP
    }

    public abstract void remove(Transaction xaction, T item) throws PersistenceException;

    public abstract void remove(Transaction xaction, SearchTerm ... terms) throws PersistenceException;

    public abstract void update(Transaction xaction, T item, Map<String,Object> state) throws PersistenceException;
    
    protected void set(Map<String,Object> map, String fieldName, Object value, Class<?> type) throws PersistenceException {
        map.put(fieldName, mapValue(fieldName, value, type, null));
    }
    
    protected void set(T target, Field field, Object value) throws PersistenceException {
        try {
            ParameterizedType pt = null;
            field.setAccessible(true);
            if( field.getGenericType() instanceof ParameterizedType ) {
                pt = (ParameterizedType)field.getGenericType();
            }
            field.set(target, mapValue(field.getName(), value, field.getType(), pt));
        }
        catch( IllegalArgumentException e ) {
            throw new PersistenceException(e);
        }
        catch( IllegalAccessException e ) {
            throw new PersistenceException(e);
        }
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Object mapValue(String fieldName, Object dataStoreValue, Class<?> toType, ParameterizedType ptype) throws PersistenceException {
        LookupDelegate delegate = getLookupDelegate(fieldName);

        if( dataStoreValue != null && delegate != null && !delegate.validate(dataStoreValue.toString()) ) {
            throw new PersistenceException("Value " + dataStoreValue + " for " + fieldName + " is not valid.");
        }
        try {
            if( toType.equals(String.class) ) {
                if( dataStoreValue != null && !(dataStoreValue instanceof String) ) {
                    dataStoreValue = dataStoreValue.toString();
                }
            }
            else if( Enum.class.isAssignableFrom(toType) ) {
                if( dataStoreValue != null ) {
                    Enum e = Enum.valueOf((Class<? extends Enum>)toType, dataStoreValue.toString());
                    
                    dataStoreValue = e;
                }
            }
            else if( toType.equals(Boolean.class) || toType.equals(boolean.class) ) {
                if( dataStoreValue == null ) {
                    dataStoreValue = false;
                }
                else if( !(dataStoreValue instanceof Boolean) ) {
                    if( Number.class.isAssignableFrom(dataStoreValue.getClass()) ) {
                        dataStoreValue = (((Number)dataStoreValue).intValue() != 0);
                    }
                    else {
                        dataStoreValue = (dataStoreValue.toString().trim().equalsIgnoreCase("true") || dataStoreValue.toString().trim().equalsIgnoreCase("y"));
                    }
                }
            }
            else if( Number.class.isAssignableFrom(toType) || toType.equals(byte.class) || toType.equals(short.class) ||
                    toType.equals(long.class) || toType.equals(int.class) || toType.equals(float.class) || toType.equals(double.class) ) {
                if( dataStoreValue == null ) {
                    if( toType.equals(int.class) || toType.equals(short.class) || toType.equals(long.class) ) {
                        dataStoreValue = 0;
                    }
                    else if( toType.equals(float.class) || toType.equals(double.class) ) {
                        dataStoreValue = 0.0f;
                    }
                }
                else if( toType.equals(Number.class) ) {
                    if( !(dataStoreValue instanceof Number) ) {
                        if( dataStoreValue instanceof String ) {
                            try {
                                dataStoreValue = Double.parseDouble((String)dataStoreValue);
                            }
                            catch( NumberFormatException e ) {
                                throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                            }
                        }
                        else if( dataStoreValue instanceof Boolean ) {
                            dataStoreValue = (((Boolean)dataStoreValue) ? 1 : 0);
                        }
                        else {
                            throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                        }
                    }                    
                }
                else if( toType.equals(Integer.class) || toType.equals(int.class) ) {
                    if( dataStoreValue instanceof Number ) {
                        if( !(dataStoreValue instanceof Integer) ) {
                            dataStoreValue = ((Number)dataStoreValue).intValue();
                        }
                    }
                    else if( dataStoreValue instanceof String ) {
                        try {
                            dataStoreValue = Integer.parseInt((String)dataStoreValue);
                        }
                        catch( NumberFormatException e ) {
                            throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                        }
                    }
                    else if( dataStoreValue instanceof Boolean ) {
                        dataStoreValue = (((Boolean)dataStoreValue) ? 1 : 0);
                    }
                    else {
                        throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);                        
                    }
                }
                else if( toType.equals(Long.class) || toType.equals(long.class) ) {
                    if( dataStoreValue instanceof Number ) {
                        if( !(dataStoreValue instanceof Long) ) {
                            dataStoreValue = ((Number)dataStoreValue).longValue();
                        }
                    }
                    else if( dataStoreValue instanceof String ) {
                        try {
                            dataStoreValue = Long.parseLong((String)dataStoreValue);
                        }
                        catch( NumberFormatException e ) {
                            throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                        }
                    }
                    else if( dataStoreValue instanceof Boolean ) {
                        dataStoreValue = (((Boolean)dataStoreValue) ? 1L : 0L);
                    }
                    else {
                        throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);                        
                    }
                }
                else if( toType.equals(Byte.class) || toType.equals(byte.class) ) {
                    if( dataStoreValue instanceof Number ) {
                        if( !(dataStoreValue instanceof Byte) ) {
                            dataStoreValue = ((Number)dataStoreValue).byteValue();
                        }
                    }
                    else if( dataStoreValue instanceof String ) {
                        try {
                            dataStoreValue = Byte.parseByte((String)dataStoreValue);
                        }
                        catch( NumberFormatException e ) {
                            throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                        }
                    }
                    else if( dataStoreValue instanceof Boolean ) {
                        dataStoreValue = (((Boolean)dataStoreValue) ? 1 : 0);
                    }
                    else {
                        throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);                        
                    }
                }
                else if( toType.equals(Short.class) || toType.equals(short.class) ) {
                    if( dataStoreValue instanceof Number ) {
                        if( !(dataStoreValue instanceof Short) ) {
                            dataStoreValue = ((Number)dataStoreValue).shortValue();
                        }
                    }
                    else if( dataStoreValue instanceof String ) {
                        try {
                            dataStoreValue = Short.parseShort((String)dataStoreValue);
                        }
                        catch( NumberFormatException e ) {
                            throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                        }
                    }
                    else if( dataStoreValue instanceof Boolean ) {
                        dataStoreValue = (((Boolean)dataStoreValue) ? 1 : 0);
                    }
                    else {
                        throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);                        
                    }
                }
                else if( toType.equals(Double.class) || toType.equals(double.class) ) {
                    if( dataStoreValue instanceof Number ) {
                        if( !(dataStoreValue instanceof Double) ) {
                            dataStoreValue = ((Number)dataStoreValue).doubleValue();
                        }
                    }
                    else if( dataStoreValue instanceof String ) {
                        try {
                            dataStoreValue = Double.parseDouble((String)dataStoreValue);
                        }
                        catch( NumberFormatException e ) {
                            throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                        }
                    }
                    else if( dataStoreValue instanceof Boolean ) {
                        dataStoreValue = (((Boolean)dataStoreValue) ? 1.0 : 0.0);
                    }
                    else {
                        throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);                        
                    }
                }
                else if( toType.equals(Float.class) || toType.equals(float.class) ) {
                    if( dataStoreValue instanceof Number ) {
                        if( !(dataStoreValue instanceof Float) ) {
                            dataStoreValue = ((Number)dataStoreValue).floatValue();
                        }
                    }
                    else if( dataStoreValue instanceof String ) {
                        try {
                            dataStoreValue = Float.parseFloat((String)dataStoreValue);
                        }
                        catch( NumberFormatException e ) {
                            throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                        }
                    }
                    else if( dataStoreValue instanceof Boolean ) {
                        dataStoreValue = (((Boolean)dataStoreValue) ? 1.0f : 0.0f);
                    }
                    else {
                        throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);                        
                    }
                }
                else if( toType.equals(BigDecimal.class) ) {
                    if( dataStoreValue instanceof Number ) {
                        if( !(dataStoreValue instanceof BigDecimal) ) {
                            if( dataStoreValue instanceof BigInteger ) {
                                dataStoreValue = new BigDecimal((BigInteger)dataStoreValue);
                            }
                            else {
                                dataStoreValue = BigDecimal.valueOf(((Number)dataStoreValue).doubleValue());
                            }
                        }
                    }
                    else if( dataStoreValue instanceof String ) {
                        try {
                            dataStoreValue = new BigDecimal((String)dataStoreValue);
                        }
                        catch( NumberFormatException e ) {
                            throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                        }
                    }
                    else if( dataStoreValue instanceof Boolean ) {
                        dataStoreValue = new BigDecimal((((Boolean)dataStoreValue) ? 1.0 : 0.0));
                    }
                    else {
                        throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);                        
                    }
                }
                else if( toType.equals(BigInteger.class) ) {
                    if( dataStoreValue instanceof Number ) {
                        if( !(dataStoreValue instanceof BigInteger) ) {
                            if( dataStoreValue instanceof BigDecimal ) {
                                dataStoreValue = ((BigDecimal)dataStoreValue).toBigInteger();
                            }
                            else {
                                dataStoreValue = BigInteger.valueOf(((Number)dataStoreValue).longValue());
                            }
                        }
                    }
                    else if( dataStoreValue instanceof String ) {
                        try {
                            dataStoreValue = new BigDecimal((String)dataStoreValue);
                        }
                        catch( NumberFormatException e ) {
                            throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);
                        }
                    }
                    else if( dataStoreValue instanceof Boolean ) {
                        dataStoreValue = new BigDecimal((((Boolean)dataStoreValue) ? 1.0 : 0.0));
                    }
                    else {
                        throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);                        
                    }
                }
                else if( dataStoreValue != null ) {
                    System.err.println("Type of dataStoreValue=" + dataStoreValue.getClass());
                    throw new PersistenceException("Unable to map " + fieldName + " as " + toType + " using " + dataStoreValue);                        
                }
            }
            else if( toType.equals(Locale.class )) {
                if( dataStoreValue != null && !(dataStoreValue instanceof Locale) ) {
                    String[] parts = dataStoreValue.toString().split("_");
                    
                    if( parts != null && parts.length > 1 ) {
                        dataStoreValue = new Locale(parts[0], parts[1]);
                    }
                    else {
                        dataStoreValue = new Locale(parts[0]);
                    }      
                }
            }
            else if( Measured.class.isAssignableFrom(toType) ) {
                if( dataStoreValue != null && ptype != null ) {
                    if( Number.class.isAssignableFrom(dataStoreValue.getClass()) ) {
                        Constructor<? extends Measured> constructor = null;
                        double value = ((Number)dataStoreValue).doubleValue();
                        
                        for( Constructor<?> c : toType.getDeclaredConstructors() ) {
                            Class[] args = c.getParameterTypes();
                            
                            if( args != null && args.length == 2 && Number.class.isAssignableFrom(args[0]) && UnitOfMeasure.class.isAssignableFrom(args[1]) ) {
                                constructor = (Constructor<? extends Measured>)c;
                                break;
                            }
                        }  
                        if( constructor == null ) {
                            throw new PersistenceException("Unable to map with no proper constructor");
                        }
                        dataStoreValue = constructor.newInstance(value, ((Class<?>)ptype.getActualTypeArguments()[0]).newInstance());
                    }
                    else if( !(dataStoreValue instanceof Measured) ) {
                        try {
                            dataStoreValue = Double.parseDouble(dataStoreValue.toString()); 
                        }
                        catch( NumberFormatException e ) {
                            Method method = null;
                            
                            for( Method m : toType.getDeclaredMethods() ) {
                                if( Modifier.isStatic(m.getModifiers()) && m.getName().equals("valueOf") ) {
                                    if( m.getParameterTypes().length == 1 && m.getParameterTypes()[0].equals(String.class) ) {
                                        method = m;
                                        break;
                                    }
                                }
                            }
                            if( method == null ) {
                                throw new PersistenceException("Don't know how to map " + dataStoreValue + " to " + toType + "<" + ptype + ">");
                            }
                            dataStoreValue = method.invoke(null, dataStoreValue.toString());
                        }
                    } 
                    // just because we converted it to a measured object above doesn't mean
                    // we have the unit of measure right
                    if( dataStoreValue instanceof Measured ) {
                        UnitOfMeasure targetUom = (UnitOfMeasure)((Class<?>)ptype.getActualTypeArguments()[0]).newInstance();
                        
                        if( !(((Measured)dataStoreValue).getUnitOfMeasure()).equals(targetUom) ) {
                            dataStoreValue = ((Measured)dataStoreValue).convertTo((UnitOfMeasure)((Class<?>)ptype.getActualTypeArguments()[0]).newInstance());
                        }
                    } 
                }
            }
            else if( toType.equals(UUID.class) ) {
                if( dataStoreValue != null && !(dataStoreValue instanceof UUID) ) {
                    dataStoreValue = UUID.fromString(dataStoreValue.toString());
                }
            }
            else if( toType.equals(TimeZone.class) ) {
                if( dataStoreValue != null && !(dataStoreValue instanceof TimeZone) ) {
                    dataStoreValue = TimeZone.getTimeZone(dataStoreValue.toString());
                }
            }
            else if( toType.equals(Currency.class) ) {
                if( dataStoreValue != null && !(dataStoreValue instanceof Currency) ) {
                    dataStoreValue = Currency.getInstance(dataStoreValue.toString());
                }
            }
            else if( toType.isArray() ) {
                Class<?> t = toType.getComponentType();
                
                if( dataStoreValue == null ) {
                    dataStoreValue = Array.newInstance(t, 0);
                }
                else if( dataStoreValue instanceof JSONArray ) {
                    JSONArray arr = (JSONArray)dataStoreValue;
                    
                    if( long.class.isAssignableFrom(t)  ) {
                        long[] replacement = (long[])Array.newInstance(long.class, arr.length());
                        
                        for( int i = 0; i<arr.length(); i++ ) {
                            replacement[i] = (Long)mapValue(fieldName, arr.get(i), t, null);
                        }
                        dataStoreValue = replacement;
                    }
                    else if( int.class.isAssignableFrom(t)  ) {
                        int[] replacement = (int[])Array.newInstance(int.class, arr.length());

                        for( int i = 0; i<arr.length(); i++ ) {
                            replacement[i] = (Integer)mapValue(fieldName, arr.get(i), t, null);
                        }
                        dataStoreValue = replacement;
                    }
                    else {
                        Object[] replacement = (Object[])Array.newInstance(t, arr.length());
                    
                        for( int i = 0; i<arr.length(); i++ ) {
                            replacement[i] = mapValue(fieldName, arr.get(i), t, null);
                        }
                        dataStoreValue = replacement;
                    }
                }
                else if( !dataStoreValue.getClass().isArray() ) {
                    logger.error("Unable to map data store type " + dataStoreValue.getClass().getName() + " to " + toType.getName());
                    logger.error("Value of " + fieldName + "=" + dataStoreValue);
                    throw new PersistenceException("Data store type=" + dataStoreValue.getClass().getName());
                }
            }
            else if( dataStoreValue != null && !toType.isAssignableFrom(dataStoreValue.getClass()) ) {
                Annotation[] alist = toType.getDeclaredAnnotations();
                boolean autoJSON = false;

                for( Annotation a : alist ) {
                    if( a instanceof AutoJSON ) {
                        autoJSON = true;
                    }
                }
                if( autoJSON ) {
                    dataStoreValue = autoDeJSON(toType, (JSONObject)dataStoreValue);
                }
                else {
                    try {
                        Method m = toType.getDeclaredMethod("valueOf", JSONObject.class);

                        dataStoreValue = m.invoke(null, dataStoreValue);
                    }
                    catch( NoSuchMethodException ignore ) {
                        try {
                            Method m = toType.getDeclaredMethod("valueOf", String.class);

                            if( m != null ) {
                                dataStoreValue = m.invoke(null, dataStoreValue.toString());
                            }
                            else {
                                throw new PersistenceException("No valueOf() field in " + toType + " for mapping " + fieldName);
                            }
                        }
                        catch( NoSuchMethodException e ) {
                            throw new PersistenceException("No valueOf() field in " + toType + " for mapping " + fieldName + " with " + dataStoreValue + ": (" + dataStoreValue.getClass().getName() + " vs " + toType.getName() + ")");
                        }
                    }
                }
            }
        }
        catch( Exception e ) {
            e.printStackTrace();
            throw new PersistenceException("Error mapping field in " + toType + " for " + fieldName + ": " + e.getMessage());
        }
        return dataStoreValue;
    }
    
    protected String toDataStoreJSONFromCurrentState(Map<String,Object> state) {
        HashMap<String,Object> friendlyState = new HashMap<String,Object>();
        Class<? extends Object> t = getTarget();
        
        while( !t.equals(Object.class) ) {
            for( Field field : t.getDeclaredFields()) {
                int modifiers = field.getModifiers();
                
                if( Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers) ) {
                    continue;
                }
                Object value = state.get(field.getName());
                
                if( value != null ) {
                    friendlyState.put(field.getName(), toJSONValue(value));
                }
            }
            t = t.getSuperclass();
        }
        friendlyState.put("SCHEMA_VERSION", getSchemaVersion());
        return new JSONObject(friendlyState).toString();
    }
    

    @SuppressWarnings("rawtypes")
    protected Object toJSONValue(Object value) {
        if( value == null ) {
            return null;
        }
        else if( value instanceof String ) {
            return value;
        }
        else if( Number.class.isAssignableFrom(value.getClass()) ) {
            return value;
        }
        else if( value instanceof Enum ) {
            return ((Enum)value).name();
        }
        else if( value instanceof Measured ) {
            return ((Measured)value).doubleValue();
        }
        else if( value instanceof Locale ) {
            Locale loc = (Locale)value;
            String str = loc.getLanguage();
            
            if( loc.getCountry() != null ) {
                str = str + "_" + loc.getCountry();
            }
            return str;
        }
        else if( value instanceof UUID ) {
            return ((UUID)value).toString();
        }
        else if( value instanceof TimeZone ) {
            return ((TimeZone)value).getID();
        }
        else if( value instanceof Currency ) {
            return ((Currency)value).getCurrencyCode();
        }
        else if( value.getClass().isArray() ) {
            Object[] replacement;

            if( value.getClass().getComponentType().isPrimitive() ) {
                if( value.getClass().getComponentType().equals(long.class)) {
                    long[] original = (long[])value;
                    
                    replacement = new Object[original.length];
                    for( int i=0; i<original.length; i++ ) {
                        replacement[i] = toJSONValue(original[i]);
                    }
                }
                else if( value.getClass().getComponentType().equals(int.class) ) {
                    int[] original = (int[])value;

                    replacement = new Object[original.length];
                    for( int i=0; i<original.length; i++ ) {
                        replacement[i] = toJSONValue(original[i]);
                    }                    
                }
                else if( value.getClass().getComponentType().equals(byte.class) ) {
                    byte[] original = (byte[])value;

                    replacement = new Object[original.length];
                    for( int i=0; i<original.length; i++ ) {
                        replacement[i] = toJSONValue(original[i]);
                    }
                }
                else if( value.getClass().getComponentType().equals(boolean.class) ) {
                    boolean[] original = (boolean[])value;

                    replacement = new Object[original.length];
                    for( int i=0; i<original.length; i++ ) {
                        replacement[i] = toJSONValue(original[i]);
                    }
                }
                else {
                    Object[] original = (Object[])value;

                    replacement = new Object[original.length];
                    // note: cannot do in-place editing because types may not match
                    // with the declared type for original
                    for( int i=0; i<original.length; i++ ) {
                        replacement[i] = toJSONValue(original[i]);
                    }
                }
            }
            else {
                Object[] original = (Object[])value;
                
                replacement = new Object[original.length];
                // note: cannot do in-place editing because types may not match
                // with the declared type for original
                for( int i=0; i<original.length; i++ ) {
                    replacement[i] = toJSONValue(original[i]);
                }
            }
            return replacement;
        }
        else if( value instanceof Collection ) {
            Collection<?> c = (Collection<?>)value;
            Object[] replacement = new Object[c.size()];
            int i =0;

            for( Object item : c ) {
                replacement[i++] = toJSONValue(item);
            }
            return replacement;
        }
        else {
            Annotation[] alist = value.getClass().getDeclaredAnnotations();
            boolean autoJSON = false;

            for( Annotation a : alist ) {
                if( a instanceof AutoJSON ) {
                    autoJSON = true;
                }
            }
            if( autoJSON ) {
                return autoJSON(value);
            }
            else {
                try {
                    Method m = value.getClass().getDeclaredMethod("toJSON");

                    return (JSONObject)m.invoke(value);
                }
                catch( Exception e ) {
                    return value.toString();
                }
            }
        }
    }

    private @Nonnull JSONObject autoJSON(@Nonnull Object ob) {
        HashMap<String,Object> json = new HashMap<String, Object>();
        Class<?> cls = ob.getClass();

        while( !(cls.getName().equals(Object.class.getName())) ) {
            Field[] fields = cls.getDeclaredFields();

            for( Field field : fields ) {
                int modifiers = field.getModifiers();

                if( Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers) ) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    Object value = field.get(ob);

                    value = toJSONValue(value);
                    json.put(field.getName(), value);
                }
                catch( IllegalAccessException e ) {
                    // this should not happen, don't map
                    logger.warn("Illegal access exception mapping " + cls.getName() + "." + field.getName() + ": " + e.getMessage(), e);
                }
            }
            cls = cls.getSuperclass();
        }
        return new JSONObject(json);
    }

    private @Nonnull <T> T autoDeJSON(@Nonnull Class<T> targetClass, @Nonnull JSONObject ob) throws PersistenceException {
        Class<?> cls = targetClass;

        T item;

        try {
            item = targetClass.newInstance();
        }
        catch( Exception e ) {
            throw new PersistenceException(e);
        }
        while( !(cls.getName().equals(Object.class.getName())) ) {
            Field[] fields = cls.getDeclaredFields();

            for( Field field : fields ) {
                int modifiers = field.getModifiers();

                if( Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers) ) {
                    continue;
                }
                field.setAccessible(true);
                Object value = null;

                if( ob.has(field.getName()) ) {
                    try {
                        value = mapValue(field.getName(), ob.get(field.getName()), field.getType(), null);
                    }
                    catch( JSONException e ) {
                        logger.warn("JSON error mapping " + targetClass.getName() + "." + field.getName() + ": " + e.getMessage(), e);
                    }
                }
                if( value != null || !field.getType().isPrimitive() ) {
                    try {
                        field.set(item, value);
                    }
                    catch( IllegalAccessException e ) {
                        logger.warn("Could not set " + targetClass.getName() + "." + field.getName() + ": " + e.getMessage(), e);
                    }
                }
            }
            cls = cls.getSuperclass();
        }
        return item;
    }

    protected Map<String,Object> toMapFromJSON(String json) throws PersistenceException {
        try {
            HashMap<String,Object> values = new HashMap<String,Object>();
            JSONObject j = new JSONObject(json);
            Class<T> targetClass = getTarget();
            Class<? extends Object> t = targetClass;
            

            while( !t.getName().equals(Object.class.getName()) ) {
                for( Field field : t.getDeclaredFields() ) {
                    int modifiers = field.getModifiers();
                    
                    if( !Modifier.isStatic(modifiers) && !Modifier.isTransient(modifiers) ) {
                        String fieldName = field.getName();
                        
                        set(values, fieldName, j.has(fieldName) ? j.get(fieldName) : null, field.getType());
                    }
                }
                t = t.getSuperclass();
            }
            return values;
        }
        catch( JSONException e ) {
            throw new PersistenceException(e);
        }
        catch( RuntimeException e ) {
            e.printStackTrace();
            throw new PersistenceException(e);
        }
        catch( Error e ) {
            e.printStackTrace();
            throw new PersistenceException(e.getMessage());
        }
    }
    
    protected @Nonnull T toTargetFromJSON(@Nonnull String schemaVersion, @Nonnull String json) throws PersistenceException {
        try {
            return toTargetFromJSON(schemaVersion, new JSONObject(json));
        }
        catch( JSONException e ) {
            throw new PersistenceException(e);
        }        
    }
    
    protected @Nonnull T toTargetFromJSON(@Nonnull String sourceVersion, @Nonnull JSONObject j) throws PersistenceException {
        try {
            while( !sourceVersion.equals(schemaVersion) ) {
                SchemaMapper mapper = getSchemaMapper(sourceVersion);

                if( mapper == null ) {
                    break;
                }
                j = mapper.map(sourceVersion, j);
                sourceVersion = mapper.getTargetVersion();
            }
            Class<T> targetClass = getTarget();
            Class<? extends Object> t = targetClass;
            T item = targetClass.newInstance();

            while( !t.getName().equals(Object.class.getName()) ) {
                for( Field field : t.getDeclaredFields() ) {
                    int modifiers = field.getModifiers();
                    
                    if( !Modifier.isStatic(modifiers) && !Modifier.isTransient(modifiers) ) {
                        Object value = (j.has(field.getName()) ? j.get(field.getName()) : null);

                        set(item, field, value);
                    }
                }
                t = t.getSuperclass();
            }
            return item;
        }
        catch( JSONException e ) {
            throw new PersistenceException(e);
        }  
        catch( InstantiationException e ) {
            throw new PersistenceException(e);
        }
        catch( IllegalAccessException e ) {
            throw new PersistenceException(e);
        }
        catch( RuntimeException e ) {
            e.printStackTrace();
            throw new PersistenceException(e);
        }
        catch( Error e ) {
            e.printStackTrace();
            throw new PersistenceException(e.getMessage());
        }
    }
    
    protected @Nonnull T toTargetFromMap(@Nonnull String dataStoreVersion, @Nonnull Map<String,Object> dataStoreState) throws PersistenceException {
        try {
            while( !dataStoreVersion.equals(schemaVersion) ) {
                SchemaMapper mapper = getSchemaMapper(dataStoreVersion);

                if( mapper == null ) {
                    break;
                }
                dataStoreState = mapper.map(dataStoreVersion, dataStoreState);
                dataStoreVersion = mapper.getTargetVersion();
            }
            Class<T> targetClass = getTarget();
            Class<? extends Object> t = targetClass;
            T item = targetClass.newInstance();

            while( !t.getName().equals(Object.class.getName()) ) {
                for( Field field : t.getDeclaredFields() ) {
                    int modifiers = field.getModifiers();
                    
                    if( !Modifier.isStatic(modifiers) && !Modifier.isTransient(modifiers) ) {
                        Object value = dataStoreState.get(field.getName());

                        set(item, field, value);
                    }
                }
                t = t.getSuperclass();
            }
            return item;
        }
        catch( InstantiationException e ) {
            throw new PersistenceException(e);
        }
        catch( IllegalAccessException e ) {
            throw new PersistenceException(e);
        }
        catch( RuntimeException e ) {
            e.printStackTrace();
            throw new PersistenceException(e);
        }
        catch( Error e ) {
            e.printStackTrace();
            throw new PersistenceException(e.getMessage());
        }
    }
    
    public void updateAll(Transaction xaction, Map<String,Object> state, SearchTerm ... terms) throws PersistenceException {
        for( T item : find(terms) ) {
            Map<String,Object> copy = new HashMap<String,Object>();
            
            copy.putAll(state);
            update(xaction, item, copy);
        }
    }
    
    /**
     * Releases all held references in the cache.
     */
    public void releaseAll() {
    	getCache().releaseAll();
    }

    static public void main(String ... args) throws Exception {
        if( args.length < 1 ) {
            System.err.println("Invalid command, syntax: java " + PersistentCache.class.getName() + " [COMMAND] ([ARGS])");
            System.exit(1);
        }
        else {
            String cmd = args[0];

            if( cmd.equalsIgnoreCase("schema") ) {
                if( args.length < 2 ) {
                    System.err.println("Invalid command, syntax: java " + PersistentCache.class.getName() + " schema [CLASS NAME]");
                    System.exit(2);
                }
                else {
                    PersistentCache<?> cache = PersistentCache.getCache((Class<? extends CachedItem>)Class.forName(args[1]));

                    System.out.println(cache.getSchema());
                }
            }
            else if( cmd.equalsIgnoreCase("reindex") ) {
                if( args.length < 2 ) {
                    System.err.println("Invalid command, syntax: java " + PersistentCache.class.getName() + " reindex [CLASS NAME]");
                    System.exit(3);
                }
                else {
                    PersistentCache<?> cache = PersistentCache.getCache((Class<? extends CachedItem>)Class.forName(args[1]));

                    cache.reindex();
                }
            }
            else if( cmd.equalsIgnoreCase("update") ) {
                if( args.length < 4 ) {
                    System.err.println("Syntax: java " + PersistentCache.class.getName() + " update [CLASS NAME]:[KEY VALUE] [FIELD] [VALUE]");
                    System.exit(4);
                }
                else {
                    String cname = args[1];
                    int idx = cname.indexOf(":");

                    if( idx < 1 ) {
                        System.err.println("Invalid class/key reference: " + cname);
                        System.exit(100);
                    }
                    else {
                        PersistentCache<CachedItem> cache = (PersistentCache<CachedItem>)PersistentCache.getCache((Class<? extends CachedItem>)Class.forName(cname.substring(0, idx)));
                        Transaction xaction = Transaction.getInstance();
                        String keyValue = cname.substring(idx + 1);
                        CachedItem item = cache.get(cname.substring(idx+1));
                        Class<?> cls = item.getClass();
                        Field field = null;

                        while( field == null && !cls.getName().equals(Object.class.getName()) ) {
                            for( Field f : cls.getDeclaredFields() ) {
                                if( f.getName().equals(args[2]) ) {
                                    field = f;
                                    break;
                                }
                            }
                            if( field == null ) {
                                cls = cls.getSuperclass();
                            }
                        }
                        if( field == null ) {
                            throw new PersistenceException("No such field: " + args[2]);
                        }
                        cache.set(item, field, args[3]);

                        Map<String,Object> state = new HashMap<String, Object>();
                        Memento<?> memento = new Memento(item);

                        memento.save(state);
                        state = memento.getState();
                        try {
                            cache.update(xaction, item, state);
                            xaction.commit();
                        }
                        finally {
                            xaction.rollback();
                        }
                    }
                }
            }
            else if( cmd.equalsIgnoreCase("help") ) {
                if( args.length < 2 ) {
                    System.out.println("Syntax: java " + PersistentCache.class.getName() + " [COMMAND] ([ARGS])");
                    System.out.println("Valid commands:");
                    System.out.println("\thelp [COMMAND]");
                    System.out.println("\treindex [CLASS NAME]");
                    System.out.println("\tschema [CLASS NAME]");
                    System.out.println("\tupdate [CLASS NAME]:[KEY VALUE] [FIELD] [VALUE]");
                }
                else if( args[1].equalsIgnoreCase("help") ) {
                    System.out.println("This is the help command");
                }
                else if( args[1].equalsIgnoreCase("schema") ) {
                    System.out.println("Syntax: java " + PersistentCache.class.getName() + " schema [CLASS NAME]");
                    System.out.println("Shows the schema for the specified class");
                }
                else if( args[1].equalsIgnoreCase("reindex") ) {
                    System.out.println("Syntax: java " + PersistentCache.class.getName() + " reindex [CLASS NAME]");
                    System.out.println("Re-indexes all values in a specific class. Useful if you have made changes to the class index annotations.");
                }
                else if( args[1].equalsIgnoreCase("update") ) {
                    System.out.println("Syntax: java " + PersistentCache.class.getName() + " update [CLASS NAME]:[KEY VALUE] [FIELD] [VALUE]");
                    System.out.println("Changes a value for the specified resource in the data store. KEY VALUE is the value of the target key to change. The FIELD is the name of the field to change. VALUE is the new value for FIELD");
                }
                else {
                    System.err.println("Unknown command: " + args[1]);
                    System.exit(5);
                }
            }
            else {
                System.err.println("Unknown command: " + cmd);
                System.err.println("Valid commands:");
                System.err.println("\thelp [COMMAND]");
                System.err.println("\treindex [CLASS NAME]");
                System.err.println("\tschema [CLASS NAME]");
                System.out.println("\tupdate [CLASS NAME]:[KEY VALUE] [FIELD] [VALUE]");
                System.exit(-1);
            }
        }
    }
}

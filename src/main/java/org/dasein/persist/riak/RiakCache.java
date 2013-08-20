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

package org.dasein.persist.riak;
                               
import java.io.IOException;
import java.io.InputStream;                                                
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.log4j.Logger;
import org.dasein.persist.DaseinSequencer;
import org.dasein.persist.Key;
import org.dasein.persist.Memento;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.PersistentCache;
import org.dasein.persist.SearchTerm;
import org.dasein.persist.Transaction;
import org.dasein.persist.jdbc.AutomatedSql.Operator;
import org.dasein.util.CacheLoader;
import org.dasein.util.CacheManagementException;
import org.dasein.util.CachedItem;
import org.dasein.util.CursorPopulator;
import org.dasein.util.ForwardCursor;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorFilter;
import org.dasein.util.JiteratorLoadException;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RiakCache<T extends CachedItem> extends PersistentCache<T> {
    static private final Logger std  = Logger.getLogger("org.dasein.persist.riak.RiakCache");
    static private final Logger wire = Logger.getLogger("org.dasein.persist.wire.riak");
    
    private String  proxyHost;
    private int     proxyPort;
    private String  riakHost;
    private int     riakPort;
    private boolean useSsl;
    
    public RiakCache() { }

    @Override
    protected void init(Class<T> cls, Key ... keys) {
        Properties props = new Properties();

        try {
            InputStream is = DaseinSequencer.class.getResourceAsStream(DaseinSequencer.PROPERTIES);

            if( is != null ) {
                props.load(is);
            }
        }
        catch( Exception e ) {
            std.error("Problem reading " + DaseinSequencer.PROPERTIES + ": " + e.getMessage(), e);
        }
        useSsl = false;
        proxyPort = 0;
        riakPort = Integer.parseInt(props.getProperty("dasein.persist.riak.port", "8089").trim());
        if( props.containsKey("dasein.persist.riak.useSsl") ) {
            useSsl = props.getProperty("dasein.persist.riak.useSsl").trim().equalsIgnoreCase("true");
        }
        String cname = cls.getName();

        while( cname != null ) {
            String prop = props.getProperty("dasein.persist.riak.host." + cname);

            if( prop != null ) {
                prop = prop.trim();
                if( prop.length() > 0 ) {
                    riakHost = prop;
                    prop = props.getProperty("dasein.persist.riak.port." + cname);
                    if( prop != null ) {
                        prop = prop.trim();
                        if( prop.length() > 0 ) {
                            riakPort = Integer.parseInt(prop);
                        }
                    }
                    if( props.containsKey("dasein.persist.riak.useSsl." + cname) ) {
                        useSsl = props.getProperty("dasein.persist.riak.useSsl." + cname).trim().equalsIgnoreCase("true");
                    }
                    break;
                }
            }
            int idx = cname.lastIndexOf(".");

            if( idx < 1 ) {
                cname = null;
            }
            else {
                cname = cname.substring(0, idx);
            }
        }
        if( riakHost == null ) {
            riakHost = props.getProperty("dasein.persist.riak.host", "localhost").trim();
        }
        if( props.containsKey("dasein.persist.riak.proxyHost") ) {
            proxyHost = props.getProperty("dasein.persist.riak.proxyHost");
            if( props.containsKey("dasein.persist.riak.proxyPort") ) {
                proxyPort = Integer.parseInt(props.getProperty("dasein.persist.riak.proxyPort"));
            }
        }
    }

    private transient volatile String bucketName;
    
    public String getBucket() {
        if( bucketName == null ) {
            String entityName = getEntityName();

            if( entityName != null ) {
                bucketName = entityName;
                return bucketName;
            }
            String name = getEntityClassName();

            int idx = name.lastIndexOf('.');

            while( idx == 0 && name.length() > 1 ) {
                name = name.substring(1);
                idx = name.lastIndexOf('.');
            }
            while( idx == (name.length()-1) && name.length() > 1 ) {
                name = name.substring(0, name.length()-1);
                idx = name.lastIndexOf('.');
            }
            if( idx == -1 || name.length() < 2 ) {
                return name;
            }
            name = name.substring(idx+1);
            StringBuilder s = new StringBuilder();

            for( int i=0; i<name.length(); i++ ) {
                char c = name.charAt(i);

                if( Character.isUpperCase(c) && i > 0 ) {
                    s.append("_");
                }
                s.append(Character.toLowerCase(c));
            }
            bucketName = s.toString();
        }
        return bucketName;
    }
    
    private transient String endpoint = null;
    
    private HttpClient getClient() {
        HttpClient client = new HttpClient();

        if( proxyHost != null ) {
            client.getHostConfiguration().setProxy(proxyHost, proxyPort);
        }
        return client;
    }

    private void addCommonRequestHeaders(HttpMethod method) {
        method.setRequestHeader("Connection", "close");
    }
    
    private String getEndpoint() {
        if( endpoint == null ) {
            StringBuilder str = new StringBuilder();
            
            str.append(useSsl ? "https://" : "http://");
            str.append(riakHost == null ? "localhost" : riakHost);
            str.append(riakPort < 1 ? ":8098" : (":" + riakPort));
            str.append("/");
            endpoint = str.toString();
        }
        return endpoint;
    }
    
    private String buildMapFunction(boolean forCounting, SearchTerm ... terms) throws PersistenceException {
        StringBuilder script = new StringBuilder();


        script.append("function(ob) { ");
        if( terms == null || terms.length < 1 ) {
            if( forCounting ) {
                script.append(" return [ 1 ]; }");
            }
            else {
                script.append(" return [ Riak.mapValuesJson(ob)[0] ]; }");
            }
        }
        else {
            boolean declare = true;

            for( SearchTerm t : terms ) {
                boolean useVal = true;
                if( declare ) {
                    script.append(" var v = Riak.mapValuesJson(ob)[0]; ");
                    declare = false;
                }
                script.append("if( v.");
                script.append(t.getColumn());
                switch( t.getOperator() ) {
                    case EQUALS: script.append(" == "); break;
                    case GREATER_THAN: script.append(" > "); break;
                    case GREATER_THAN_OR_EQUAL_TO: script.append(" >= "); break;
                    case LESS_THAN: script.append(" < "); break;
                    case LESS_THAN_OR_EQUAL_TO: script.append(" <= "); break;
                    case NOT_EQUAL: script.append(" != "); break;
                    case LIKE: script.append(".toLowerCase().match(/" + t.getValue().toString().toLowerCase() + "/i)"); useVal = false; break;
                    case NOT_NULL: script.append(" != null"); useVal = false; break;
                    case NULL: script.append(" == null"); useVal = false; break;
                    default: throw new PersistenceException("Unsupported operator: " + t.getOperator());
                }
                if( useVal ) {
                    Object value = toJSONValue(t.getValue());

                    if( value instanceof Long || value instanceof Short || value instanceof Integer || value instanceof Byte ) {
                        script.append(String.valueOf(((Number)value).longValue()));
                    }
                    else if( value instanceof Double || value instanceof Float ) {
                        script.append(String.valueOf(((Number)value).doubleValue()));
                    }
                    else if( value instanceof BigInteger || value instanceof BigDecimal ) {
                        script.append(value.toString());
                    }
                    else {
                        script.append("'" + value.toString() + "'");
                    }
                }
                script.append(" ) { ");
            }
            script.append(" return [" + (forCounting ? "1" : "v") + "]; ");
            for( @SuppressWarnings("unused") SearchTerm t : terms ) {
                script.append(" } ");
            }
            script.append(" return []; }");
        }
        return script.toString();
    }
    
    private String buildReduceSort(boolean desc, String ... fields) throws PersistenceException {
        StringBuilder script = new StringBuilder();
        
        script.append("function(v) { ");
        script.append("v.sort(function(left, right) { ");
        for( String field : fields ) {
            script.append("if( left." + field + "");
            if( desc ) {
                script.append(" < ");
            }
            else {
                script.append(" > ");
            }
            script.append("right." + field + " ) { return 1; }");
            script.append("else if( left." + field + "");
            if( desc ) {
                script.append(" > ");
            }
            else {
                script.append(" < ");
            }
            script.append("right." + field + " ) { return -1; }");
        }
        script.append(" return 0;");
        script.append("}");
        script.append("); }");
        return script.toString();
    }
    
    @Override 
    public long count() throws PersistenceException {
        JSONObject ob = findKeysInBucketAsJSON();
        
        if( ob.has("keys") ) {
            try {
                return ob.getJSONArray("keys").length();
            }
            catch( JSONException e ) {
                throw new PersistenceException(e);
            }
        }
        return 0;
    }
    
    @Override
    public long count(SearchTerm ... terms) throws PersistenceException {
        if( terms == null || terms.length < 1 ) {
            return count();
        }
        if( wire.isDebugEnabled() ) {
            startCall("count");
        }
        try {
            String mapFunction = buildMapFunction(true, terms);
            
            HashMap<String,Object> request = new HashMap<String,Object>();
            HashMap<String,Object> inputs = new HashMap<String,Object>();

            terms = matchKeys(inputs, terms);
            if( inputs.size() < 1 ) {
                request.put("inputs", getBucket());
            }
            else {
                inputs.put("bucket", getBucket());
                request.put("inputs", inputs);
            }

            ArrayList<Map<String,Object>> query = new ArrayList<Map<String,Object>>();
            HashMap<String,Object> maps = new HashMap<String,Object>();
            HashMap<String,Object> reduces = new HashMap<String,Object>();
            HashMap<String,Object> map = new HashMap<String,Object>();
            HashMap<String,Object> reduce = new HashMap<String,Object>();
            
            map.put("language", "javascript");
            map.put("source", mapFunction);
            map.put("keep", true);
            maps.put("map", map);
            
            reduce.put("language", "javascript");
            reduce.put("keep", true);
            reduce.put("name", "Riak.reduceSum");
            reduces.put("reduce", reduce);
            
            query.add(maps);
            query.add(reduces);
            request.put("query", query);
            
            String json = (new JSONObject(request)).toString();
            
            HttpClient client = getClient();
            PostMethod post = new PostMethod(getEndpoint() + "mapred");
            addCommonRequestHeaders(post);
            int code;
            
            try {
                post.setRequestEntity(new StringRequestEntity(json, "application/json", "utf-8"));
                if( wire.isDebugEnabled() ) {
                    try {
                        wire.debug(post.getName() + " " + getEndpoint() + "mapred");
                        wire.debug("");
                        for( Header h : post.getRequestHeaders() ) {
                            wire.debug(h.getName() + ": " + h.getValue());
                        }
                        wire.debug("Content-length: " + post.getRequestEntity().getContentLength());
                        wire.debug("Content-type: " + post.getRequestEntity().getContentType());
                        wire.debug("");
                        wire.debug(((StringRequestEntity)post.getRequestEntity()).getContent());
                        wire.debug("");
                    }
                    catch( Throwable ignore ) {
                        // ignore
                    }
                }
                code = client.executeMethod(post);
            }
            catch( HttpException e ) {
                throw new PersistenceException("HttpException during POST: " + e.getMessage());
            }
            catch( IOException e ) {
                throw new PersistenceException("IOException during POST: " + e.getMessage());
            }
            try {
                String body = post.getResponseBodyAsString();

                try {
                    if( wire.isDebugEnabled() ) {
                        wire.debug("----------------------------------------");
                        wire.debug("");
                        wire.debug(post.getStatusLine().getStatusCode() + " " + post.getStatusLine().getReasonPhrase());
                        wire.debug("");
                        if( body != null ) {
                            wire.debug(body);
                            wire.debug("");
                        }
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
                if( code != HttpStatus.SC_OK ) {
                    if( code == HttpStatus.SC_NOT_FOUND ) {
                        return 0;
                    }
                    throw new PersistenceException(code + ": " + body);
                }
                
                JSONArray results = new JSONArray(body);
                
                return results.getJSONArray(0).getLong(0);
            }
            catch( Exception e ) {
                throw new PersistenceException(e);
            } 
            catch( Throwable t ) {
                throw new PersistenceException(t.getMessage());
            } 
        }
        finally {
            endCall("count");
        }
    }

    private class KeyMap {
        public String keyName;
        public Object keyValue;
    }

    private @Nullable KeyMap getKeyMap(@Nonnull Key key, @Nonnull Object ... values) throws PersistenceException {
        if( key.getFields().length > 1 ) {
            int len = key.getFields().length;

            StringBuilder n = new StringBuilder();
            StringBuilder v = new StringBuilder();

            for( int i=0; i<len; i++ ) {
                Object ob = toJSONValue(values[i]);

                if( ob == null ) {
                    v.append("=*=");
                }
                else {
                    v.append(ob.toString());
                }
                n.append(key.getFields()[i].toLowerCase());
                if( i < len-1 ) {
                    n.append("-");
                    v.append("\n");
                }
            }
            KeyMap keyMap = new KeyMap();

            keyMap.keyName =  n.toString() + "_bin";
            try {
                keyMap.keyValue = Base64.encodeBase64String(v.toString().getBytes("utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                throw new PersistenceException(e);
            }
            return keyMap;
        }
        Object ob = toJSONValue(values[0]);

        if( ob != null ) {
            if( ob instanceof Integer || ob instanceof Long || ob instanceof Short ) {
                KeyMap keyMap = new KeyMap();

                keyMap.keyName = key.getFields()[0].toLowerCase() + "_int";
                keyMap.keyValue = ob.toString().trim();
                return keyMap;
            }
            else if( ob.getClass().equals(long.class) || ob.getClass().equals(int.class) || ob.getClass().equals(boolean.class) || ob.getClass().equals(byte.class) ) {
                KeyMap keyMap = new KeyMap();

                keyMap.keyName = key.getFields()[0].toLowerCase() + "_int";
                keyMap.keyValue = ob.toString().trim();
                return keyMap;
            }
            else if( ob.getClass().isArray() ) {
                Object[] items = (Object[])ob;
                KeyMap keyMap = new KeyMap();
                boolean binary;

                ob = (items.length < 1 ? null : items[0]);
                if( ob != null && (ob instanceof Integer || ob instanceof Long || ob instanceof Short) ) {
                    keyMap.keyName = key.getFields()[0].toLowerCase() + "_int";
                    binary = false;
                }
                else if( ob != null && (ob.getClass().equals(long.class) || ob.getClass().equals(int.class) || ob.getClass().equals(boolean.class) || ob.getClass().equals(byte.class)) ) {
                    keyMap.keyName = key.getFields()[0].toLowerCase() + "_int";
                    binary = false;
                }
                else {
                    keyMap.keyName = key.getFields()[0].toLowerCase() + "_bin";
                    binary = true;
                }
                ArrayList<String> keyValues = new ArrayList<String>();

                for( Object item : items ) {
                    if( item != null ) {
                        if( binary ) {
                            try {
                                String encoded = Base64.encodeBase64String(item.toString().getBytes("utf-8"));

                                keyValues.add(encoded.trim());
                            }
                            catch( UnsupportedEncodingException e ) {
                                std.error("No such encoding UTF-8: " + e.getMessage(), e);
                                throw new PersistenceException(e);
                            }
                        }
                        else {
                            keyValues.add(item.toString().trim());
                        }
                    }
                }
                keyMap.keyValue = keyValues.toArray(new String[keyValues.size()]);
                return keyMap;
            }
            else {
                try {
                    String encoded = Base64.encodeBase64String(ob.toString().getBytes("utf-8"));
                    KeyMap keyMap = new KeyMap();

                    keyMap.keyName = key.getFields()[0].toLowerCase() + "_bin";
                    keyMap.keyValue = encoded.trim();
                    return keyMap;
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("No such encoding UTF-8: " + e.getMessage(), e);
                    throw new PersistenceException(e);
                }
            }
        }
        return null;
    }

    @Override
    public T create(Transaction xaction, Map<String, Object> state) throws PersistenceException {
        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + RiakCache.class.getName() + ".create(" + xaction + "," + state + ")");
        }
        try {
            if( wire.isDebugEnabled() ) {
                startCall("create");
            }
            try {
                StringBuilder url = new StringBuilder();
                String key = getPrimaryKeyField();
                Object keyValue = state.get(key);
                
                url.append(getEndpoint());
                url.append("buckets/");
                url.append(getBucket());
                url.append("/keys/");
                url.append(keyValue);
                
                HttpClient client = getClient();
                PostMethod post = new PostMethod(url.toString());
                addCommonRequestHeaders(post);
                int code;

                try {
                    String json = toDataStoreJSONFromCurrentState(state);
                    
                    for( Key secondaryKey : getSecondaryKeys() ) {
                        if( secondaryKey.getFields().length > 1 ) {
                            int len = secondaryKey.getFields().length;

                            StringBuilder n = new StringBuilder();
                            StringBuilder v = new StringBuilder();

                            for( int i=0; i<len; i++ ) {
                                Object ob = toJSONValue(state.get(secondaryKey.getFields()[i]));

                                if( ob == null ) {
                                    v.append("=*=");
                                }
                                else {
                                    v.append(ob.toString());
                                }
                                n.append(secondaryKey.getFields()[i].toLowerCase());
                                if( i < len-1 ) {
                                    n.append("-");
                                    v.append("\n");
                                }
                            }
                            post.addRequestHeader("x-riak-index-" + n.toString() + "_bin", Base64.encodeBase64String(v.toString().getBytes("utf-8")));
                        }
                        Object ob = toJSONValue(state.get(secondaryKey.getFields()[0]));

                        if( ob != null ) {
                            if( ob instanceof Integer || ob instanceof Long ) {
                                post.addRequestHeader("x-riak-index-" + secondaryKey.getFields()[0] + "_int", ob.toString().trim());
                            }
                            else if( ob.getClass().isArray() ) {
                                Object[] items = (Object[])ob;

                                for( Object item : items ) {
                                    if( item != null ) {
                                        boolean binary = true;

                                        if( Number.class.isAssignableFrom(item.getClass()) ) {
                                            binary = false;
                                        }
                                        else if( item.getClass().equals(long.class) || item.getClass().equals(int.class) || item.getClass().equals(boolean.class) || item.getClass().equals(byte.class) ) {
                                            binary = false;
                                        }
                                        if( binary ) {
                                            try {
                                                String encoded = Base64.encodeBase64String(item.toString().getBytes("utf-8"));

                                                post.addRequestHeader("x-riak-index-" + secondaryKey.getFields()[0].toLowerCase() + "_bin", encoded.trim());
                                            }
                                            catch( UnsupportedEncodingException e ) {
                                                std.error("No such encoding UTF-8: " + e.getMessage(), e);
                                                throw new PersistenceException(e);
                                            }
                                        }
                                        else {
                                            post.addRequestHeader("x-riak-index-" + secondaryKey.getFields()[0].toLowerCase() + "_int", item.toString().trim());
                                        }
                                    }
                                }
                            }
                            else {
                                try {
                                    String encoded = Base64.encodeBase64String(ob.toString().getBytes("utf-8"));


                                    post.addRequestHeader("x-riak-index-" + secondaryKey.getFields()[0].toLowerCase() + "_bin", encoded.trim());
                                }
                                catch( UnsupportedEncodingException e ) {
                                    std.error("No such encoding UTF-8: " + e.getMessage(), e);
                                    throw new PersistenceException(e);
                                }
                            }
                            Class<? extends CachedItem> link = secondaryKey.getIdentifies();

                            if( secondaryKey.getFields().length < 2 && link != null ) {
                                try {
                                    PersistentCache<? extends CachedItem> cache = PersistentCache.getCache(link);

                                    if( cache != null && (cache instanceof RiakCache) ) {
                                        RiakCache<? extends CachedItem> c = (RiakCache<? extends CachedItem>)cache;
                                        String bucket = c.getBucket();

                                        post.addRequestHeader("Link", "</buckets/" + bucket + "/keys/" + ob.toString() +">; riaktag=\"" + secondaryKey.getFields()[0] + "\"");
                                    }
                                }
                                catch( Throwable t ) {
                                    std.warn("Unable to determine relationship status for " + secondaryKey.getFields()[0] + ": " + t.getMessage());
                                }
                            }
                        }
                    }
                    post.setRequestEntity(new StringRequestEntity(json, "application/json", "utf-8"));
                    if( wire.isDebugEnabled() ) {
                        try {
                            wire.debug(post.getName() + " " + url.toString());
                            wire.debug("");
                            for( Header h : post.getRequestHeaders() ) {
                                wire.debug(h.getName() + ": " + h.getValue());
                            }
                            wire.debug("Content-length: " + post.getRequestEntity().getContentLength());
                            wire.debug("Content-type: " + post.getRequestEntity().getContentType());
                            wire.debug("");
                            wire.debug(((StringRequestEntity)post.getRequestEntity()).getContent());
                            wire.debug("");
                        }
                        catch( Throwable ignore ) {
                            // ignore
                        }
                    }
                    code = client.executeMethod(post);
                }
                catch( HttpException e ) {
                    std.error("HTTP exception during POST: " + e.getMessage());
                    throw new PersistenceException("HttpException during POST: " + e.getMessage());
                }
                catch( IOException e ) {
                    std.error("I/O exception during POST: " + e.getMessage());
                    throw new PersistenceException("I/O exception during POST: " + e.getMessage()); 
                }
                try {
                    String body = post.getResponseBodyAsString();
                    
                    try {
                        if( wire.isDebugEnabled() ) {
                            wire.debug("----------------------------------------");
                            wire.debug("");
                            wire.debug(post.getStatusLine().getStatusCode() + " " + post.getStatusLine().getReasonPhrase());
                            wire.debug("");
                            if( body != null ) {
                                wire.debug(body);
                                wire.debug("");
                            }
                        }
                    }
                    catch( Throwable ignore ) {
                        // ignore
                    }
                    if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_NOT_FOUND ) {
                        std.warn("Failed attempt to create Riak object (" + code + "): " + body);
                        throw new PersistenceException(code + ": " + body);
                    }
                    return get(keyValue);
                }
                catch( IOException e ) {
                    std.error("Error talking to Riak server: " + e.getMessage());
                    throw new PersistenceException(e);
                } 
            }
            finally {
                endCall("create");
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("EXIT: " + RiakCache.class.getName() + ".create()");
            }
        }
    }

    @Override
    public @Nonnull Collection<T> find(@Nonnull SearchTerm[] terms, @Nullable final JiteratorFilter<T> filter, @Nullable Boolean orderDesc, @Nullable String... orderFields) throws PersistenceException {
        return (Collection<T>)execFind(false, terms, filter, orderDesc, orderFields);
    }

    @Override
    public @Nonnull ForwardCursor<T> findAsCursor(@Nonnull SearchTerm[] terms, @Nullable final JiteratorFilter<T> filter, @Nullable Boolean orderDesc, @Nullable String ... orderFields) throws PersistenceException {
        return (ForwardCursor<T>)execFind(true, terms, filter, orderDesc, orderFields);
    }

    private Iterable<T> execFind(boolean cursor, @Nonnull SearchTerm[] terms, @Nullable final JiteratorFilter<T> filter, @Nullable Boolean orderDesc, @Nullable String ... orderFields) throws PersistenceException {
        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + RiakCache.class.getName() + ".find(" + Arrays.toString(terms) + "," + filter + "," + orderDesc + "," + Arrays.toString(orderFields) + ")");
        }
        try {
            if( (orderFields == null || orderFields.length < 1) && (terms.length == 1 || (terms.length == 2 && terms[1].getValue() != null && terms[0].getValue() != null && Number.class.isAssignableFrom(terms[0].getValue().getClass()) && (terms[1].getValue() instanceof Boolean))) ) {
                boolean equals = true;
                
                for( SearchTerm t : terms ) {
                    if( !t.getOperator().equals(Operator.EQUALS) ) {
                        equals = false;
                        break;
                    }
                }
                if( equals ) {
                    Key key = matchKeys(terms);
                    
                    if( key != null ) {
                        StringBuilder url = new StringBuilder();
                        String value;
                        
                        url.append(getEndpoint());
                        url.append("buckets/");
                        url.append(getBucket());
                        url.append("/index/");
                        for( int i=0; i<key.getFields().length; i++ ) {
                            url.append(key.getFields()[i].toLowerCase());
                            if( i < key.getFields().length-1 ) {
                                url.append("-");
                            }
                        }
                        url.append("_");
                        try {
                            if( key.getFields().length > 1 ) {
                                StringBuilder v = new StringBuilder();
                                
                                url.append("bin");
                                for( int i=0; i<key.getFields().length; i++ ) {
                                    String f = key.getFields()[i];
                                    
                                    for( SearchTerm t : terms ) {
                                        if( t.getColumn().equalsIgnoreCase(f) ) {
                                            Object ob = t.getValue();

                                            if( ob == null ) {
                                                ob = "=*=";
                                            }
                                            v.append(ob.toString());
                                            if( i < key.getFields().length-1 ) {
                                                v.append("\n");
                                            }
                                            break;
                                        }
                                    }
                                }
                                value = Base64.encodeBase64String(v.toString().getBytes("utf-8"));
                            }
                            else if( terms[0].getValue() == null || (!(terms[0].getValue() instanceof Long) && !(terms[0].getValue() instanceof Integer) && !(terms[0].getValue() instanceof Short)) ) {
                                url.append("bin");
                                value = Base64.encodeBase64String((terms[0].getValue() == null ? "" : terms[0].getValue().toString()).getBytes("utf-8"));
                            }
                            else {
                                url.append("int");
                                value = String.valueOf(((Number)terms[0].getValue()).longValue());
                            }
                        }
                        catch( UnsupportedEncodingException e ) {
                            throw new PersistenceException(e);
                        }
                        url.append("/");
                        url.append(value);
                        if( filter == null ) {
                            return list(cursor, url.toString(), null);
                        }
                        else {
                            return list(cursor, url.toString(), filter);
                        }
                    }
                }
            }
            startCall("findWithMapReduce");
            try {
                HashMap<String,Object> request = new HashMap<String,Object>();
                ArrayList<Map<String,Object>> query = new ArrayList<Map<String,Object>>();
                HashMap<String,Object> maps = new HashMap<String,Object>();
                HashMap<String,Object> map = new HashMap<String,Object>();
                HashMap<String,Object> inputs = new HashMap<String, Object>();

                terms = matchKeys(inputs, terms);
                if( inputs.size() < 1 ) {
                    request.put("inputs", getBucket());
                }
                else {
                    inputs.put("bucket", getBucket());
                    request.put("inputs", inputs);
                }
                map.put("language", "javascript");
                map.put("source", buildMapFunction(false, terms));
                map.put("keep", true);
                maps.put("map", map);
                
                query.add(maps);
                if( orderFields != null && orderFields.length > 0 ) {
                    HashMap<String,Object> reduces = new HashMap<String,Object>();
                    HashMap<String,Object> reduce = new HashMap<String,Object>();

                    reduce.put("language", "javascript");
                    reduce.put("keep", true);
                    reduce.put("source", buildReduceSort(orderDesc != null && orderDesc, orderFields));
                    reduces.put("reduce", reduce);
                
                    query.add(reduces);
                }
                request.put("query", query);
                String json = (new JSONObject(request)).toString();

                HttpClient client = getClient();
                PostMethod post = new PostMethod(getEndpoint() + "mapred");
                addCommonRequestHeaders(post);
                int code;
                
                try {
                    post.setRequestEntity(new StringRequestEntity(json, "application/json", "utf-8"));
                    if( wire.isDebugEnabled() ) {
                        try {
                            wire.debug(post.getName() + " " + getEndpoint() + "mapred");
                            wire.debug("");
                            for( Header h : post.getRequestHeaders() ) {
                                wire.debug(h.getName() + ": " + h.getValue());
                            }
                            wire.debug("Content-length: " + post.getRequestEntity().getContentLength());
                            wire.debug("Content-type: " + post.getRequestEntity().getContentType());
                            wire.debug("");
                            wire.debug(((StringRequestEntity)post.getRequestEntity()).getContent());
                            wire.debug("");
                        }
                        catch( Throwable ignore ) {
                            // ignore
                        }
                    }
                    code = client.executeMethod(post);
                }
                catch( HttpException e ) {
                    throw new PersistenceException("HttpException during POST: " + e.getMessage());
                }
                catch( IOException e ) {
                    throw new PersistenceException("IOException during POST: " + e.getMessage());
                }
                try {
                    String body = post.getResponseBodyAsString();

                    try {
                        if( wire.isDebugEnabled() ) {
                            wire.debug("----------------------------------------");
                            wire.debug("");
                            wire.debug(post.getStatusLine().getStatusCode() + " " + post.getStatusLine().getReasonPhrase());
                            wire.debug("");
                            if( body != null ) {
                                wire.debug(body);
                                wire.debug("");
                            }
                        }
                    }
                    catch( Throwable ignore ) {
                        // ignore
                    }
                    if( code != HttpStatus.SC_OK ) {
                        if( code == HttpStatus.SC_NOT_FOUND ) {
                            return Collections.emptyList();
                        }
                        throw new PersistenceException(code + ": " + body);
                    }
                    final JSONArray results = ((orderFields != null && orderFields.length > 0) ? (new JSONArray(body)).getJSONArray(0) : new JSONArray(body));
                    final int len = results.length();

                    if( cursor ) {
                        CursorPopulator<T> populator = new CursorPopulator<T>(getTarget().getName() + ".find", null) {
                            @Override
                            public void populate(ForwardCursor<T> cursor) {
                                try {
                                    for( int i=0; i<len; i++ ) {
                                        JSONObject ob = results.getJSONObject(i);

                                        if( std.isDebugEnabled() ) {
                                            std.debug("find - checking cache for " + ob.get(getPrimaryKeyField()));
                                        }
                                        T item = getCache().find(getPrimaryKeyField(), ob.get(getPrimaryKeyField()));

                                        if( item == null ) {
                                            if( std.isDebugEnabled() ) {
                                                std.debug("find - cache miss, loading " + ob.get(getPrimaryKeyField()));
                                            }
                                            String version = "0";

                                            if( ob.has("SCHEMA_VERSION") ) {
                                                version = ob.getString("SCHEMA_VERSION");
                                            }
                                            item = toTargetFromJSON(version, ob);
                                            if( item != null ) {
                                                getCache().cache(item);
                                            }
                                        }
                                        if( item != null ) {
                                            try {
                                                if( filter == null || filter.filter(item) ) {
                                                    cursor.push(item);
                                                }
                                            }
                                            catch( Throwable t ) {
                                                throw new RuntimeException(t);
                                            }
                                        }
                                    }
                                }
                                catch( Throwable t ) {
                                    throw new JiteratorLoadException(t);
                                }
                            }
                        };

                        populator.populate();
                        if( filter == null ) {
                            populator.setSize(len);
                        }
                        return populator.getCursor();
                    }
                    else {
                        PopulatorThread<T> populator = new PopulatorThread<T>(new JiteratorPopulator<T>() {
                            @Override
                            public void populate(@Nonnull Jiterator<T> iterator) throws Exception {
                                for( int i=0; i<len; i++ ) {
                                    JSONObject ob = results.getJSONObject(i);

                                    if( std.isDebugEnabled() ) {
                                        std.debug("find - checking cache for " + ob.get(getPrimaryKeyField()));
                                    }
                                    T item = getCache().find(getPrimaryKeyField(), ob.get(getPrimaryKeyField()));

                                    if( item == null ) {
                                        if( std.isDebugEnabled() ) {
                                            std.debug("find - cache miss, loading " + ob.get(getPrimaryKeyField()));
                                        }
                                        String version = "0";

                                        if( ob.has("SCHEMA_VERSION") ) {
                                            version = ob.getString("SCHEMA_VERSION");
                                        }
                                        item = toTargetFromJSON(version, ob);
                                        if( item != null ) {
                                            getCache().cache(item);
                                        }
                                    }
                                    if( item != null ) {
                                        try {
                                            if( filter == null || filter.filter(item) ) {
                                                iterator.push(item);
                                            }
                                        }
                                        catch( Throwable t ) {
                                            throw new RuntimeException(t);
                                        }
                                    }
                                }
                            }
                        });
                        populator.populate();
                        if( filter == null ) {
                            populator.setSize(len);
                        }
                        return populator.getResult();
                    }
                }
                catch( Exception e ) {
                    std.error(e.getMessage(), e);
                    throw new PersistenceException(e);
                }
            }
            finally {
                endCall("findWithMapReduce");
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("EXIT: " + RiakCache.class.getName() + ".find()");
            }            
        }
    }

    /*
    private JSONString findKeysInBucket() throws PersistenceException {
        return new JSONString() {
            @Override
            public String toJSONString() {
                try {
                    JSONObject ob = findKeysInBucketAsJSON();
                    JSONArray keys = ob.getJSONArray("keys");

                    if( keys.length() < 1 ) {
                        return null;
                    }
                    StringBuilder b = new StringBuilder();
                        
                    b.append("[");
                    for( int i=0; i<keys.length(); i++ ) {
                        b.append("[");
                        b.append("\"");
                        b.append(getBucket());
                        b.append("\",\"");
                        b.append(keys.getString(i));
                        b.append("\"");
                        b.append("],");
                    }
                    b.append("]");
                    return b.toString();
                }
                catch( Exception e ) {
                    throw new RuntimeException(e);
                }
            }
        };       
    }
    */

    public void reindex() {
        JSONObject ob;

        try {
            ob = findKeysInBucketAsJSON();
        }
        catch( PersistenceException e ) {
            std.warn("Unable to re-index: " + e.getMessage(), e);
            return;
        }
        if( ob.has("keys") ) {
            try {
                JSONArray keys = ob.getJSONArray("keys");

                for( int i=0; i<keys.length(); i++ ) {
                    String key = keys.getString(i);
                    try {
                        T item = get(key);

                        if( item != null ) {
                            reindex(item);
                        }
                    }
                    catch( Throwable t ) {
                        std.warn("Unable to re-index: " + t.getMessage(), t);
                    }
                }
            }
            catch( Throwable t ) {
                std.warn("Unable to re-index: " + t.getMessage(), t);
            }
        }
    }

    private void reindex(T item) {
        Transaction xaction = Transaction.getInstance();

        try {
            try {
                std.info("Re-indexing: " + item);

                Map<String,Object> state = new HashMap<String, Object>();
                Memento<?> memento = new Memento(item);

                memento.save(state);
                update(xaction, item, memento.getState());
                xaction.commit();
            }
            catch( Throwable t ) {
                std.warn("Unable to re-index " + item + ": " + t.getMessage(), t);
            }
        }
        finally {
            xaction.rollback();
        }
    }

    private JSONObject findKeysInBucketAsJSON() throws PersistenceException {
        startCall("findKeysInBucket");
        try {
            StringBuilder url = new StringBuilder();
            
            url.append(getEndpoint());
            url.append("buckets/");
            url.append(getBucket());
            url.append("/index/$bucket/");
            url.append(getBucket());
            
            HttpClient client = getClient();
            GetMethod get = new GetMethod(url.toString());
            addCommonRequestHeaders(get);
            int code;
            
            if( wire.isDebugEnabled() ) {
                try {
                    wire.debug(get.getName() + " " + url.toString());
                    wire.debug("");
                    for( Header h : get.getRequestHeaders() ) {
                        wire.debug(h.getName() + ": " + h.getValue());
                    }
                    wire.debug("");
                }
                catch( Throwable ignore ) {
                    // ignore
                }
            }
            try {
                code = client.executeMethod(get);
            }
            catch( HttpException e ) {
                throw new PersistenceException("HttpException during GET: " + e.getMessage());
            }
            catch( IOException e ) {
                throw new PersistenceException("IOException during GET: " + e.getMessage());
            }
            try {
                String json = get.getResponseBodyAsString();
                
                try {
                    if( wire.isDebugEnabled() ) {
                        wire.debug("----------------------------------------");
                        wire.debug("");
                        wire.debug(get.getStatusLine().getStatusCode() + " " + get.getStatusLine().getReasonPhrase());
                        wire.debug("");
                        if( json != null ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
                if( code != HttpStatus.SC_OK ) {
                    if( code == HttpStatus.SC_NOT_FOUND ) {
                        return new JSONObject("{\"keys\" : []}");
                    }
                    else {
                        throw new PersistenceException(code + ": " + json);
                    }
                }
                return new JSONObject(json);
            }
            catch( JSONException e ) {
                throw new PersistenceException(e);
            }
            catch( IOException e ) {
                std.error("Failed to load JSON key list from Riak: " + e.getMessage());
                throw new PersistenceException(e);
            }
        }
        finally {
            endCall("findKeysInBucket");
        } 
    }
    
    @Override
    public T get(Object keyValue) throws PersistenceException {
        if( keyValue == null ) {
            return null;
        }
        final String primaryKey = keyValue.toString();
        
        try {
            CacheLoader<T> loader;
            
            loader = new CacheLoader<T>() {
                public T load(Object ... args ) {
                    startCall("loadObject");
                    try {
                        if( std.isDebugEnabled() ) {
                            std.debug("get - cache miss, loading " + primaryKey);
                        }
                        StringBuilder url = new StringBuilder();
                        
                        url.append(getEndpoint());
                        url.append("buckets/");
                        url.append(getBucket());
                        url.append("/keys/");
                        url.append(primaryKey);

                        HttpClient client = getClient();
                        GetMethod get = new GetMethod(url.toString());
                        addCommonRequestHeaders(get);
                        int code;
                        
                        try {
                            if( wire.isDebugEnabled() ) {
                                try {
                                    wire.debug(get.getName() + " " + url.toString());
                                    wire.debug("");
                                    for( Header h : get.getRequestHeaders() ) {
                                        wire.debug(h.getName() + ": " + h.getValue());
                                    }
                                    wire.debug("");
                                }
                                catch( Throwable ignore ) {
                                    // ignore
                                }
                            }
                            code = client.executeMethod(get);
                        }
                        catch( HttpException e ) {
                            throw new RuntimeException("HttpException during GET: " + e.getMessage());
                        }
                        catch( IOException e ) {
                            throw new RuntimeException("IOException during GET: " + e.getMessage());
                        }
                        try {
                            final String body = get.getResponseBodyAsString();
                            
                            if( wire.isDebugEnabled() ) {
                                try {
                                    wire.debug("----------------------------------------");
                                    wire.debug("");
                                    wire.debug(get.getStatusLine().getStatusCode() + " " + get.getStatusLine().getReasonPhrase());
                                    wire.debug("");
                                    if( body != null ) {
                                        wire.debug(body);
                                        wire.debug("");
                                    }
                                }
                                catch( Throwable ignore ) {
                                    // ignore
                                }
                            }
                            if( code != HttpStatus.SC_OK ) {
                                if( code == HttpStatus.SC_NOT_FOUND ) {
                                    return null;
                                }
                                throw new RuntimeException(code + ": " + body);
                            }
                            JSONObject ob = new JSONObject(body);
                            String version = "0";

                            if( ob.has("SCHEMA_VERSION") ) {
                                version = ob.getString("SCHEMA_VERSION");
                            }
                            return toTargetFromJSON(version, ob);
                        }
                        catch( IOException e ) {
                            throw new RuntimeException(e);
                        }  
                        catch( PersistenceException e ) {
                            throw new RuntimeException(e);
                        }
                        catch( JSONException e ) {
                            throw new RuntimeException(e);
                        }
                    }
                    finally {
                        endCall("loadObject");
                    }
                }
            };
            if( std.isDebugEnabled() ) {
                std.debug("get - looking in cache for " + keyValue);
            }
            return getCache().find(getPrimaryKeyField(), keyValue, loader, getPrimaryKeyField(), keyValue);
        }
        catch( CacheManagementException e ) {
            throw new PersistenceException(e);
        }
        catch( RuntimeException e ) {
            Throwable t = e.getCause();
            
            if( t != null && t instanceof PersistenceException ) {
                throw (PersistenceException)t;
            }
            throw new PersistenceException(e);
        }
    }

    @Override
    public String getSchema() throws PersistenceException {
        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + RiakCache.class.getName() + ".getSchema()");
        }
        try {
            StringBuilder url = new StringBuilder();
            StringBuilder indexes = new StringBuilder();

            String key = getPrimaryKeyField();

            url.append(getEndpoint());
            url.append("buckets/");
            url.append(getBucket());
            url.append("/keys/");
            url.append("$").append(getPrimaryKeyField());

            HashMap<String,Class<?>> fields = new HashMap<String, Class<?>>();
            Class<?> cls = getTarget();

            while( !cls.getName().equals(Object.class.getName()) ) {
                for( Field field : cls.getDeclaredFields()) {
                    int modifiers = field.getModifiers();

                    if( Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers) ) {
                        continue;
                    }
                    Class<?> t = field.getType();

                    fields.put(field.getName(), t);
                }
                cls = cls.getSuperclass();
            }
            for( Key secondaryKey : getSecondaryKeys() ) {
                if( secondaryKey.getFields().length > 1 ) {
                    int len = secondaryKey.getFields().length;

                    StringBuilder n = new StringBuilder();

                    for( int i=0; i<len; i++ ) {
                        n.append(secondaryKey.getFields()[i].toLowerCase());
                        if( i < len-1 ) {
                            n.append("-");
                        }
                    }
                    indexes.append(n.toString()).append("_bin: ");
                    for( String field : secondaryKey.getFields() ) {
                        indexes.append(field).append(",");
                    }
                    indexes.append("\n");
                }
                else {
                    Class<?> c = fields.get(secondaryKey.getFields()[0]);

                    if( c != null && ((c.isPrimitive() && (c.equals(long.class) || c.equals(int.class) || c.equals(short.class))) || (c.equals(Long.class) || c.equals(Integer.class) || c.equals(Short.class))) ) {
                        indexes.append(secondaryKey.getFields()[0].toLowerCase()).append("_int: ").append(secondaryKey.getFields()[0]).append("\n");
                    }
                    else {
                        indexes.append(secondaryKey.getFields()[0].toLowerCase()).append("_bin: ").append(secondaryKey.getFields()[0]).append("\n");
                    }
                }
            }

            HashMap<String,Object> schema = new HashMap<String, Object>();

            for( Map.Entry<String,Class<?>> entry : fields.entrySet() ) {
                if( entry.getValue().isPrimitive() ) {
                    schema.put(entry.getKey(), entry.getValue().getName());
                }
                else {
                    schema.put(entry.getKey(), entry.getValue().getSimpleName());
                }
                /*
                else if( Number.class.isAssignableFrom(entry.getValue()) ) {
                    schema.put(entry.getKey(), entry.getValue().getSimpleName());
                }
                else if( entry.getValue().equals(String.class) || entry.getValue().equals(Boolean.class) || Enum.class.isAssignableFrom(entry.getValue()) ) {
                    schema.put(entry.getKey(), entry.getValue().getSimpleName());
                }
                else if( Locale.class.isAssignableFrom(entry.getValue()) || Currency.class.isAssignableFrom(entry.getValue()) ) {
                    schema.put(entry.getKey(), entry.getValue().getSimpleName());
                }
                */
            }
            try {
                return (url.toString() + "\nSchema: " + getSchemaVersion() + "\n\n" + indexes.toString() + "\n" + ((new JSONObject(schema))).toString(4));
            }
            catch( JSONException e ) {
                throw new PersistenceException(e);
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("EXIT: " + RiakCache.class.getName() + ".getSchema()");
            }
        }
    }

    @Override
    public @Nonnull Collection<T> list() throws PersistenceException {
        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + RiakCache.class.getName() + ".list()");
        }
        try {
            StringBuilder url = new StringBuilder();
            
            url.append(getEndpoint());
            url.append("buckets/");
            url.append(getBucket());
            url.append("/index/$bucket/");
            url.append(getBucket());
            return (Collection<T>)list(false, url.toString(), null);
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("EXIT: " + RiakCache.class.getName() + ".list()");
            }            
        }
    }

    @Override
    public @Nonnull ForwardCursor<T> listAsCursor() throws PersistenceException {
        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + RiakCache.class.getName() + ".list()");
        }
        try {
            StringBuilder url = new StringBuilder();

            url.append(getEndpoint());
            url.append("buckets/");
            url.append(getBucket());
            url.append("/index/$bucket/");
            url.append(getBucket());
            return (ForwardCursor<T>)list(true, url.toString(), null);
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("EXIT: " + RiakCache.class.getName() + ".list()");
            }
        }
    }

    private @Nonnull Iterable<T> list(boolean asCursor, @Nonnull String listEndpoint, final @Nullable JiteratorFilter<T> filter) throws PersistenceException {
        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + RiakCache.class.getName() + ".list(" + listEndpoint + ")");
        }
        try {
            startCall("list");
            try {
                HttpClient client = getClient();
                GetMethod get = new GetMethod(listEndpoint);
                addCommonRequestHeaders(get);
                int code;
                
                if( wire.isDebugEnabled() ) {
                    try {
                        wire.debug(get.getName() + " " + listEndpoint);
                        wire.debug("");
                        for( Header h : get.getRequestHeaders() ) {
                            wire.debug(h.getName() + ": " + h.getValue());
                        }
                        wire.debug("");
                    }
                    catch( Throwable ignore ) {
                        // ignore
                    }
                }
                try {
                    code = client.executeMethod(get);
                }
                catch( HttpException e ) {
                    throw new PersistenceException("HttpException during GET: " + e.getMessage());
                }
                catch( IOException e ) {
                    throw new PersistenceException("IOException during GET: " + e.getMessage());
                }
                try {
                    final String body = get.getResponseBodyAsString();
                    
                    try {
                        if( wire.isDebugEnabled() ) {
                            wire.debug("----------------------------------------");
                            wire.debug("");
                            wire.debug(get.getStatusLine().getStatusCode() + " " + get.getStatusLine().getReasonPhrase());
                            wire.debug("");
                            if( body != null ) {
                                wire.debug(body);
                                wire.debug("");
                            }
                        }
                    }
                    catch( Throwable ignore ) {
                        // ignore
                    }
                    if( code != HttpStatus.SC_OK ) {
                        if( code == HttpStatus.SC_NOT_FOUND ) {
                            return Collections.emptyList();
                        }
                        throw new PersistenceException(code + ": " + body);
                    }
                    JSONObject ob = new JSONObject(body);

                    if( ob.has("keys") ) {
                        final JSONArray keys = ob.getJSONArray("keys");
                        final int len = keys.length();

                        if( asCursor ) {
                            CursorPopulator<T> populator = new CursorPopulator<T>(getTarget().getName() + ".list", null) {
                                @Override
                                public void populate(ForwardCursor<T> cursor) {
                                    try {
                                        for( int i=0; i<len; i++ ) {
                                            String key = keys.getString(i);

                                            T item = get(key);

                                            if( item != null ) {
                                                try {
                                                    if( filter == null || filter.filter(item) ) {
                                                        cursor.push(item);
                                                    }
                                                }
                                                catch( Throwable t ) {
                                                    throw new JiteratorLoadException(t);
                                                }
                                            }
                                        }
                                    }
                                    catch( JSONException e ) {
                                        throw new JiteratorLoadException(e);
                                    }
                                    catch( PersistenceException e ) {
                                        throw new JiteratorLoadException(e);
                                    }
                                }
                            };

                            populator.populate();
                            if( filter == null ) {
                                populator.setSize(len);
                            }
                            return populator.getCursor();
                        }
                        else {
                            PopulatorThread<T> populator;

                            populator = new PopulatorThread<T>(new JiteratorPopulator<T>() {
                                public void populate(@Nonnull Jiterator<T> iterator) throws Exception {
                                    for( int i=0; i<len; i++ ) {
                                        String key = keys.getString(i);

                                        T item = get(key);

                                        if( item != null ) {
                                            try {
                                                if( filter == null || filter.filter(item) ) {
                                                    iterator.push(item);
                                                }
                                            }
                                            catch( Throwable t ) {
                                                throw new RuntimeException(t);
                                            }
                                        }
                                    }
                                }
                            });
                            populator.populate();
                            if( filter == null ) {
                                populator.setSize(len);
                            }
                            return populator.getResult();
                        }
                    }
                    return Collections.emptyList();
                }
                catch( IOException e ) {
                    throw new PersistenceException(e);
                }
                catch( JSONException e ) {
                    throw new PersistenceException(e);
                }
            }
            finally {
                endCall("list");
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("EXIT: " + RiakCache.class.getName() + ".list()");
            }
        }
    }

    private @Nullable SearchTerm[] matchKeys(@Nonnull Map<String,Object> input, @Nullable SearchTerm[] terms) throws PersistenceException {
        if( terms == null ) {
            return null;
        }
        if( terms.length == 1 ) {
            Key pk = getPrimaryKey();

            if( pk.getFields().length == 1 && terms[0].getOperator().equals(Operator.EQUALS) && pk.getFields()[0].equals(terms[0].getColumn()) ) {
                KeyMap map = getKeyMap(pk, terms[0].getValue());

                if( map != null && map.keyValue instanceof String ) {
                    input.put("index", map.keyName);
                    input.put("key", map.keyValue);
                    return null;
                }
            }
            for( Key key : getSecondaryKeys() ) {
                if( key.getFields().length == 1 && terms[0].getOperator().equals(Operator.EQUALS) &&  key.getFields()[0].equals(terms[0].getColumn()) ) {
                    KeyMap map = getKeyMap(key, terms[0].getValue());

                    if( map != null && map.keyValue instanceof String ) {
                        input.put("index", map.keyName);
                        input.put("key", map.keyValue);
                        return null;
                    }
                }
            }
            return terms;
        }
        else if( terms.length == 2 && terms[0].getValue() != null && terms[1].getValue() != null ) {
            Key key = null;

            for( Key k : getSecondaryKeys() ) {
                if( k.getFields().length == 2 ) {
                    if( k.getFields()[0].equals(terms[0].getColumn()) && k.getFields()[0].equals(terms[1].getColumn()) ) {
                        key = k;
                        break;
                    }
                    if( k.getFields()[1].equals(terms[0].getColumn()) && k.getFields()[1].equals(terms[1].getColumn()) ) {
                        key = k;
                        break;
                    }
                }
            }
            if( key != null ) {
                Operator one = terms[0].getOperator();
                Operator two = terms[1].getOperator();
                Object[] range = new Object[] { null, null };

                if( one.equals(Operator.LESS_THAN_OR_EQUAL_TO) && two.equals(Operator.GREATER_THAN_OR_EQUAL_TO) ) {
                    range[0] = terms[1].getValue();
                    range[1] = terms[0].getValue();
                }
                else if( two.equals(Operator.LESS_THAN_OR_EQUAL_TO) && one.equals(Operator.GREATER_THAN_OR_EQUAL_TO) ) {
                    range[0] = terms[0].getValue();
                    range[1] = terms[1].getValue();
                }
                if( range[0] != null && range[1] != null ) {
                    KeyMap keyMap = getKeyMap(key, range);

                    if( keyMap != null ) {
                        String[] values = (String[])keyMap.keyValue;

                        input.put("index", keyMap.keyName);
                        if( keyMap.keyName.endsWith("int") ) {
                            input.put("start", Long.parseLong(values[0]));
                            input.put("end", Long.parseLong(values[1]));
                        }
                        else {
                            input.put("start", values[0]);
                            input.put("end", values[1]);
                        }
                        return null;
                    }
                }
            }
        }
        Key bestKey = null;

        for( Key key : getSecondaryKeys() ) {
            String[] indexFields = key.getFields();

            if( indexFields.length <= terms.length ) {
                boolean match = true;

                for( int idx=0; idx<indexFields.length; idx++ ) {
                    if( !terms[idx].getOperator().equals(Operator.EQUALS) || !terms[idx].getColumn().equals(indexFields[idx])) {
                        match = false;
                        break;
                    }
                }
                if( match && (bestKey == null || key.getFields().length > bestKey.getFields().length) ) {
                    bestKey = key;
                }
            }
        }
        if( bestKey != null ) {
            ArrayList<SearchTerm> newTerms = new ArrayList<SearchTerm>();
            Object[] values = new Object[bestKey.getFields().length];
            int i = 0;

            for( String field : bestKey.getFields() ) {
                for( SearchTerm term : terms ) {
                    if( term.getColumn().equals(field) ) {
                        values[i++] = term.getValue();
                        break;
                    }
                }
            }
            for( SearchTerm term : terms ) {
                boolean part = false;

                for( String field : bestKey.getFields() ) {
                    if( term.getColumn().equals(field) ) {
                        part = true;
                        break;
                    }
                }
                if( !part ) {
                    newTerms.add(term);
                }
            }
            KeyMap map = getKeyMap(bestKey, values);

            input.put("index", map.keyName);
            input.put("key", map.keyValue);
            return newTerms.toArray(new SearchTerm[newTerms.size()]);
        }
        return null;
    }

    @Override
    public void remove(Transaction xaction, T item) throws PersistenceException {
        startCall("remove");
        try {
            StringBuilder url = new StringBuilder();
            
            url.append(getEndpoint());
            url.append("buckets/");
            url.append(getBucket());
            url.append("/keys/");
            url.append(getKeyValue(item));
            
            HttpClient client = getClient();
            DeleteMethod delete = new DeleteMethod(url.toString());
            addCommonRequestHeaders(delete);
            int code;
            
            try {
                if( wire.isDebugEnabled() ) {
                    try {
                        wire.debug(delete.getName() + " " + url.toString());
                        wire.debug("");
                        for( Header h : delete.getRequestHeaders() ) {
                            wire.debug(h.getName() + ": " + h.getValue());
                        }
                        wire.debug("");
                    }
                    catch( Throwable ignore ) {
                        // ignore
                    }
                }
                code = client.executeMethod(delete);
            }
            catch( HttpException e ) {
                throw new PersistenceException("HttpException during GET: " + e.getMessage());
            }
            catch( IOException e ) {
                throw new PersistenceException("IOException during GET: " + e.getMessage());
            }
            try {
                String body = delete.getResponseBodyAsString();
                
                if( wire.isDebugEnabled() ) {
                    try {
                        wire.debug("----------------------------------------");
                        wire.debug("");
                        wire.debug(delete.getStatusLine().getStatusCode() + " " + delete.getStatusLine().getReasonPhrase());
                        wire.debug("");
                        if( body != null ) {
                            wire.debug(body);
                            wire.debug("");
                        }
                    }
                    catch( Throwable ignore ) {
                        // ignore
                    }
                }
                if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_NOT_FOUND ) {
                    throw new PersistenceException(code + ": " + body);
                }
                getCache().release(item);
            }
            catch( IOException e ) {
                throw new PersistenceException(e);
            }        
        }
        finally {
            endCall("remove");
        }
    }

    @Override
    public void remove(Transaction xaction, SearchTerm... terms) throws PersistenceException {
        for( T item : find(terms) ) {
            remove(xaction, item);
        }
    }

    /*
todo: remap
    public void remap() {

    }

    private void remap(@Nonnull Object keyValue) throws PersistenceException {
        StringBuilder url = new StringBuilder();

        url.append(getEndpoint());
        url.append("buckets/");
        url.append(getBucket());
        url.append("/keys/");
        url.append(keyValue.toString());

        HttpClient client = getClient();
        GetMethod get = new GetMethod(url.toString());
        int code;

        try {
            if( wire.isDebugEnabled() ) {
                try {
                    wire.debug(get.getName() + " " + url.toString());
                    wire.debug("");
                    for( Header h : get.getRequestHeaders() ) {
                        wire.debug(h.getName() + ": " + h.getValue());
                    }
                    wire.debug("");
                }
                catch( Throwable ignore ) {
                    // ignore
                }
            }
            code = client.executeMethod(get);
        }
        catch( HttpException e ) {
            throw new RuntimeException("HttpException during GET: " + e.getMessage());
        }
        catch( IOException e ) {
            throw new RuntimeException("IOException during GET: " + e.getMessage());
        }
        try {
            final String body = get.getResponseBodyAsString();

            if( wire.isDebugEnabled() ) {
                try {
                    wire.debug("----------------------------------------");
                    wire.debug("");
                    wire.debug(get.getStatusLine().getStatusCode() + " " + get.getStatusLine().getReasonPhrase());
                    wire.debug("");
                    if( body != null ) {
                        wire.debug(body);
                        wire.debug("");
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
            }
            if( code != HttpStatus.SC_OK ) {
                if( code == HttpStatus.SC_NOT_FOUND ) {
                    return;
                }
                std.warn("Unable to remap: " + code + " - " + body);
                return;
            }
            T item = toTargetFromJSON(body);
            Memento<T> memento = new Memento<T>(item);

            memento.save(new HashMap<String, Object>());

            Transaction xaction = Transaction.getInstance();

            try {
                update(xaction, item, memento.getState());
                xaction.commit();
            }
            finally {
                xaction.rollback();
            }
        }
        catch( IOException e ) {
            std.warn("Unable to remap: " + e.getMessage(), e);
        }
    }
    */

    @Override
    public void update(Transaction xaction, T item, Map<String, Object> state) throws PersistenceException {
        String newKey = getKeyValue(state, getPrimaryKey());
        String oldKey = getKeyValue(item);
        
        if( newKey == null || newKey.equals(oldKey) ) {
            if( newKey == null ) {
                state.put(getPrimaryKeyField(), getValue(item, getPrimaryKeyField()));                
            }
            create(xaction, state);
        }
        else {
            remove(xaction, item);
            create(xaction, state);
        }
        getCache().release(item);
    }
    
    private void endCall(String f) {
        if( wire.isDebugEnabled() ) {
            StringBuilder output = new StringBuilder();
            String pre = ">>> " + f + "() - [" + (new SimpleDateFormat("HH.mm.ss.SSS")).format(new Date()) + "] ";
            int count = 120 - pre.length();
            
            output.append(pre);
            while( (count--) > 0 ) {
                output.append("=");
            }
            output.append("==|");
            wire.debug(output.toString());
        }
    }
    
    private void startCall(String f) {
        if( wire.isDebugEnabled() ) {
            StringBuilder output = new StringBuilder();
            String pre = "|== " + f + "() - [" + (new SimpleDateFormat("HH.mm.ss.SSS")).format(new Date()) + "] ";
            int count = 120 - pre.length();
            
            output.append(pre);
            while( (count--) > 0 ) {
                output.append("=");
            }
            output.append(">>>");
            wire.debug(output.toString());            
        }
    }
}

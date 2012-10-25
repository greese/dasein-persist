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

package org.dasein.persist.attributes;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import org.dasein.attributes.AttributeMap;
import org.dasein.attributes.DataTypeFactory;
import org.dasein.attributes.DataTypeMap;
import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;
import org.dasein.util.Translator;
import org.apache.log4j.Logger;

public class CreateAttributes extends Execution {
    private static final Logger logger = Logger.getLogger(CreateAttributes.class);

    static private final int TYPE_CLASS  = 1;
    static private final int TYPE_ID     = 2;
    static private final int OWNER_CLASS = 3;
    static private final int OWNER_ID    = 4;
    static private final int NAME        = 5;
    static private final int LANGUAGE    = 6;
    static private final int COUNTRY     = 7;
    static private final int VALUE       = 8;
    static private final int ORDER       = 9;
    
    private String sql = null;
    
    public synchronized String getStatement() throws SQLException {
        if( sql == null ) {
        	StringBuilder str = new StringBuilder();
            
            str.append("INSERT INTO ");
            str.append(getIdentifier(LoadAttributes.ATTRIBUTE_TABLE));
            str.append(" ( ");
            str.append(getIdentifier("type_class"));
            str.append(", ");
            str.append(getIdentifier("type_id"));
            str.append(", ");
            str.append(getIdentifier("owner_class"));
            str.append(", ");
            str.append(getIdentifier("owner_id"));
            str.append(", ");
            str.append(getIdentifier("name"));
            str.append(", ");
            str.append(getIdentifier("language"));
            str.append(", ");
            str.append(getIdentifier("country"));
            str.append(", ");
            str.append(getIdentifier("text_value"));
            str.append(", ");
            str.append(getIdentifier("value_order"));
            str.append(" ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? )");
            sql = str.toString();
        }
        return sql;
    }
    
    public Map<String, Object> run(Transaction xaction, Map<String,Object> params) throws PersistenceException, SQLException {
        AttributeMap attrs = (AttributeMap)params.get(AttributeDAO.ATTRIBUTES);
        DataTypeMap types = (DataTypeMap)params.get(AttributeDAO.TYPES);
        String ocls = (String)params.get(AttributeDAO.OWNER_CLASS);
        String oid = (String)params.get(AttributeDAO.OWNER_ID);
        String tcls = (String)params.get(AttributeDAO.TYPE_CLASS);
        String tid = (String)params.get(AttributeDAO.TYPE_ID);
        
        if( tid == null ) {
            tid = "";
        }
        for( Map.Entry<String,Object> entry : attrs.entrySet() ) {
            String attr = entry.getKey();
            Object val = entry.getValue();
            
            if( val instanceof Collection ) {
                int i = 1;
                
                for( Object ob : (Collection<?>)val ) {
                    if( ob != null ) {
                        save(types, tcls, tid, ocls, oid, attr, ob, i++);
                    }
                }
            }
            else {
                if( val != null ) {
                    save(types, tcls, tid, ocls, oid, attr, val, 1);
                }
            }
        }
        return null;
    }

    private void save(DataTypeMap types, String tcls, String tid, String ocls, String oid, String attr, Object val, int order) throws PersistenceException, SQLException {

        if (types == null || !types.containsKey(attr)) {
            logger.warn("Owner Class: " +  ocls + " Owner ID: " + oid + " Attribute: " + attr + " doesn't have a type! Please check the types for Type Class: " + tcls + " Type ID: " + tid);
            return;
        }

        DataTypeFactory<?> factory = types.get(attr).getFactory();
        if( val instanceof Translator ) {
            Translator<?> trans = (Translator<?>)val;
            Iterator<String> languages = trans.languages();
            
            while( languages.hasNext() ) {
                String lang = languages.next();
                Iterator<String> countries;
                boolean nullctry = false;
                
                countries = trans.countries(lang);
                while( countries.hasNext() ) {
                    String ctry = countries.next();
                    Locale loc;
                    Object ob;

                    if( ctry == null || ctry.equals("") ) {
                        if( nullctry ) {
                            continue;
                        }
                        loc = new Locale(lang);
                        nullctry = true;
                        ctry = null;
                    }
                    else {
                        loc = new Locale(lang, ctry);
                    }
                    ob = trans.getExactTranslation(loc).getData();
                    if( ob == null && !nullctry ) {
                        loc = new Locale(lang);
                        ob = trans.getExactTranslation(loc).getData(); 
                        nullctry = true;
                    }
                    save(tcls, tid, ocls, oid, attr, lang, ctry, order, factory.getStringValue(ob));
                }
            }
        }
        else {
            save(tcls, tid, ocls, oid, attr, null, null, order, factory.getStringValue(val));
        }
    }

    private void save(String tcls, String tid, String ocls, String oid, String att, String lang, String ctry, int order, String val) throws PersistenceException, SQLException {
        statement.setString(TYPE_CLASS, tcls);
        statement.setString(TYPE_ID, tid);
        statement.setString(OWNER_CLASS, ocls);
        statement.setString(OWNER_ID, oid);
        statement.setString(NAME, att);
        if( lang == null ) {
            statement.setNull(LANGUAGE, Types.VARCHAR);
        }
        else {
            statement.setString(LANGUAGE, lang);
        }
        if( ctry == null ) {
            statement.setNull(COUNTRY, Types.VARCHAR);
        }
        else {
            statement.setString(COUNTRY, ctry);
        }
        statement.setString(VALUE, val);
        statement.setInt(ORDER, order);
        statement.executeUpdate();
        statement.clearParameters();
    }
}
    

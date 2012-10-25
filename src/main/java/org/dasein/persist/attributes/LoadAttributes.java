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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.dasein.attributes.AttributeMap;
import org.dasein.attributes.DataType;
import org.dasein.attributes.DataTypeFactory;
import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;
import org.dasein.util.Translator;

public class LoadAttributes extends Execution {
    static final String ATTRIBUTE_TABLE = "dsn_attribute";
    
    private String sql = null;
    
    public synchronized String getStatement() throws SQLException {
        if( sql == null ) {
            StringBuilder str = new StringBuilder();
            
            str.append("SELECT ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "name"));
            str.append(", ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "text_value"));
            str.append(", ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "language"));
            str.append(", ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "country"));
            str.append(", ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "value_order"));
            str.append(", ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "data_type"));
            str.append(", ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "type_parameters"));
            str.append(", ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "required"));
            str.append(", ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "multi_lingual"));
            str.append(", ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "multi_valued"));
            str.append(" FROM ");
            str.append(getIdentifier(ATTRIBUTE_TABLE));
            str.append(", ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE));
            str.append(" WHERE ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "owner_class"));
            str.append(" = ? AND ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "owner_id"));
            str.append(" = ? AND ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "type_class"));
            str.append(" = ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "type_class"));
            str.append(" AND ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "name"));
            str.append(" = ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "name"));
            str.append(" AND (");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "type_id"));
            str.append(" = ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "type_id"));
            str.append(" OR ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "type_id"));
            str.append(" = '' ) ");
            str.append(" ORDER BY ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "group"));
            str.append(", ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE, "index"));
            str.append(", ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "name"));
            str.append(", ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "value_order"));
            str.append(", ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "language"));
            str.append(", ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "country"));
            sql = str.toString();
        }
        return sql;
    }
    
    static private final int OBJECT_TYPE = 1;
    static private final int OBJECT_ID   = 2;

    static private final int NAME          = 1;
    static private final int VALUE         = 2;
    static private final int LANGUAGE      = 3;
    static private final int COUNTRY       = 4;
    static private final int ORDER         = 5;
    static private final int TYPE          = 6;
    static private final int PARAMETERS    = 7;
    static private final int REQUIRED      = 8;
    static private final int MULTI_LINGUAL = 9;
    static private final int MULTI_VALUED  = 10;
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<String, Object> run(Transaction xaction, Map<String,Object> params) throws PersistenceException, SQLException {
        HashMap<String,Object> attributes = new HashMap<String,Object>();
        String cname = (String)params.get(AttributeDAO.OWNER_CLASS);
        String id = (String)params.get(AttributeDAO.OWNER_ID);

        statement.setString(OBJECT_TYPE, cname);
        statement.setString(OBJECT_ID, id);
        ResultSet results = statement.executeQuery();
        
        try {
            while( results.next() ) {
                String attr = results.getString(NAME);
                String val = results.getString(VALUE);
                String lang, ctry, subtype, str;
                boolean required, ml, mv;
                DataType<? extends Object> type;
                DataTypeFactory<?> factory;
                String[] args;
                int order;
    
                lang = results.getString(LANGUAGE);
                if( results.wasNull() ) {
                    lang = null;
                }
                ctry = results.getString(COUNTRY);
                if( results.wasNull() ) {
                    ctry = null;
                }
                order = results.getInt(ORDER);
                str = results.getString(TYPE);
                factory = DataTypeFactory.getInstance(str);
                subtype = results.getString(PARAMETERS);
                if( subtype == null || results.wasNull() ) {
                    subtype = null;
                }
                if( subtype != null ) {
                    String[] tmp = subtype.split(":");
                    
                    if( tmp == null || tmp.length < 1 ) {
                        args = new String[1];
                        args[0] = subtype; 
                    }
                    else {
                        args = new String[tmp.length];
                        for(int i=0; i<tmp.length; i++) {
                            args[i] = tmp[i];
                        }
                    }
                }
                else {
                    args = null;
                }
                required = results.getString(REQUIRED).trim().equalsIgnoreCase("Y");
                ml = results.getString(MULTI_LINGUAL).trim().equalsIgnoreCase("Y");
                mv = results.getString(MULTI_VALUED).trim().equalsIgnoreCase("Y");
                type = factory.getType(ml, mv, required, args);
                if( mv ) {
                    ArrayList<Object> list = (ArrayList<Object>)attributes.get(attr);
                    
                    if( list == null ) {
                        list = new ArrayList<Object>();
                        attributes.put(attr, list);
                    }
                    if( ml ) {
                        Translator<?> curr;
                        Locale loc;
                                   
                        if( lang == null ) {
                            loc = Locale.getDefault();
                        }
                        else if( ctry == null ) {
                            loc = new Locale(lang);
                        }
                        else {
                            loc = new Locale(lang, ctry);
                        }
                        if( order == list.size() ) {
                            curr = (Translator<?>)list.get(order-1);
                        }
                        else {
                            curr = null;
                        }
                        list.add(order-1, type.getTranslatedValue(val, loc, curr));
                    }
                    else {
                        if (val != null) {
                            String[] values = val.split(",");
                            if (values.length > 0) {
                                for (String v : values) {
                                    list.add(new AttributeMap.AttributeWrapper(type, v));
                                }
                            }
                        }
                        if (list.isEmpty()) {
                            list.add(new AttributeMap.AttributeWrapper(type, val));
                        }
                    }
                }
                else {
                    if( ml ) {
                        Locale loc;
                        
                        if( lang == null ) {
                            loc = Locale.getDefault();
                        }
                        else if( ctry == null ) {
                            loc = new Locale(lang);
                        }
                        else {
                            loc = new Locale(lang, ctry);
                        }
                        attributes.put(attr, type.getTranslatedValue(val, loc, (Translator< ? >)attributes.get(attr)));
                    }
                    else {
                        attributes.put(attr, new AttributeMap.AttributeWrapper(type, val)); 
                    }
                }
            }
        }
        finally {
            try { results.close(); }
            catch( SQLException e ) { }
        }
        return attributes;
    }
}

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

package org.dasein.persist.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;
import org.dasein.util.Translator;

public abstract class TranslatorLoader extends TranslationSql {
    private String sql = null;
    
    static public final int ATTRIBUTE   = 1;
    static public final int LANGUAGE    = 2;
    static public final int COUNTRY     = 3;
    static public final int TRANSLATION = 4;
    
    static public final int W_OWNER_ID    = 1;
    
    public String getStatement() throws SQLException {
        if( sql == null ) {
            StringBuilder str = new StringBuilder();
            String tbl = getTable();
            
            str.append("SELECT ");
            str.append(getIdentifier(tbl, "attribute"));
            str.append(", ");
            str.append(getIdentifier(tbl, "language"));
            str.append(", ");
            str.append(getIdentifier(tbl, "country"));
            str.append(", ");
            str.append(getIdentifier(tbl, "translation"));
            str.append(" FROM ");
            str.append(getIdentifier(tbl));
            str.append(" WHERE ");
            str.append(getIdentifier(tbl, "owner_id"));
            str.append(" = ?");
            if( getConnection() == null ) {
                return str.toString();
            }
            sql = str.toString();
        }
        return sql;
    }
    
    public abstract String getTable();
    
    public Map<String,Object> run(Transaction ignore, Map<String,Object> state) throws PersistenceException, SQLException {
        Map<String,Map<Locale,String>> tmp = new HashMap<String,Map<Locale,String>>();
        Map<String,Object> list = new HashMap<String,Object>();
        Object id = state.get("ownerId");
        
        statement.setString(W_OWNER_ID, id.toString());
        ResultSet results = statement.executeQuery();
        
        try {
            while( results.next() ) {
                String attr = results.getString(ATTRIBUTE);
                String lang = results.getString(LANGUAGE);
                String ctry = results.getString(COUNTRY);
                Map<Locale,String> trans;
                Locale loc;
                
                if( results.wasNull() ) {
                    ctry = null;
                }
                if( ctry == null ) {
                    loc = new Locale(lang);
                }
                else {
                    loc = new Locale(lang, ctry.toUpperCase());
                }
                if( tmp.containsKey(attr) ) {
                    trans = tmp.get(attr);
                }
                else {
                    trans = new HashMap<Locale,String>();
                    tmp.put(attr, trans);
                }
                trans.put(loc, results.getString(TRANSLATION));
            }
        }
        finally {
            try { results.close(); }
            catch( SQLException e ) { }
        }
        for( String attr : tmp.keySet() ) {
            Map<Locale,String> trans = tmp.get(attr);

            list.put(attr, new Translator<String>(trans));
        }
        return list;
    }    

}

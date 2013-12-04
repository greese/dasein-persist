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

/* $Id: LoadTranslator.java,v 1.4 2007/02/24 19:27:48 greese Exp $ */
/* Copyright (c) 2005 Valtira Corporation, All Rights Reserved */
package org.dasein.persist.dao;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;
import org.dasein.util.Translator;

/**
 * <p>
 *   LoadTranslator
 *   TODO Document this class.
 * </p>
 * <p>
 *   Last modified: $Date: 2007/02/24 19:27:48 $
 * </p>
 * @version $Revision: 1.4 $
 * @author george
 */
public class LoadTranslator extends Execution {

    private String sql = null;
    
    static public final int ATTRIBUTE   = 1;
    static public final int LANGUAGE    = 2;
    static public final int COUNTRY     = 3;
    static public final int TRANSLATION = 4;
    
    static public final int W_OWNER_CLASS = 1;
    static public final int W_OWNER_ID    = 2;
    
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
            str.append(getIdentifier(tbl, "owner_class"));
            str.append(" = ? AND ");
            str.append(getIdentifier(tbl, "owner_id"));
            str.append(" = ?");
            if( getConnection() == null ) {
                return str.toString();
            }
            sql = str.toString();
        }
        return sql;
    }
    
    public String getTable() {
        return "dsn_translation";
    }
    
    public Map<String,Object> run(Transaction ignore, Map<String,Object> state) throws PersistenceException, SQLException {
        Transaction xaction = Transaction.getInstance();
        
        try {
            Map<String,Map<Locale,String>> tmp = new HashMap<String,Map<Locale,String>>();
            Map<String,Object> list = new HashMap<String,Object>();
            Class cls = (Class)state.get("ownerClass");
            Object id = state.get("ownerId");
            
            statement.setString(W_OWNER_CLASS, cls.getName());
            statement.setString(W_OWNER_ID, id.toString());
            results = statement.executeQuery();
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
            xaction.commit();
            for( String attr : tmp.keySet() ) {
                Map<Locale,String> trans = tmp.get(attr);
    
                list.put(attr, new Translator<String>(trans));
            }
            return list;
        }
        finally {
            xaction.rollback();
        }
    }
    
    @Override
	public boolean isReadOnly() {
		return true;
	}
}

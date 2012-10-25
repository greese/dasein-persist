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

/* $Id: SaveTranslator.java,v 1.3 2006/06/02 19:42:13 greese Exp $ */
/* Copyright (c) 2005 Valtira Corporation, All Rights Reserved */
package org.dasein.persist.dao;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
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
 *   Last modified: $Date: 2006/06/02 19:42:13 $
 * </p>
 * @version $Revision: 1.3 $
 * @author george
 */
public class SaveTranslator extends Execution {
    static public final int OWNER_CLASS = 1;
    static public final int OWNER_ID    = 2;
    static public final int ATTRIBUTE   = 3;
    static public final int LANGUAGE    = 4;
    static public final int COUNTRY     = 5;
    static public final int TRANSLATION = 6;
    
    private String sql = null;
    
    public String getStatement() throws SQLException {
        if( sql == null ) {
            StringBuilder str = new StringBuilder();
            String tbl = getTable();
            
            str.append("INSERT INTO ");
            str.append(getIdentifier(tbl));
            str.append(" ( ");
            str.append(getIdentifier("owner_class"));
            str.append(", ");
            str.append(getIdentifier("owner_id"));
            str.append(", ");
            str.append(getIdentifier("attribute"));
            str.append(", ");
            str.append(getIdentifier("language"));
            str.append(", ");
            str.append(getIdentifier("country"));
            str.append(", ");
            str.append(getIdentifier("translation"));
            str.append(" ) VALUES ( ?, ?, ?, ?, ?, ? )");
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
    
    public Map<String, Object> run(Transaction xaction, Map<String,Object> params) throws PersistenceException, SQLException {
        Translator<?> trans = (Translator<?>)params.get("translation");
        String cls = (String)params.get("ownerClass");
        Object id = params.get("ownerId");
        String attr = (String)params.get("attribute");
        Iterator<String> it = trans.languages();
        
        while( it.hasNext() ) {
            String lang = it.next();

            if( lang != null ) {
                Iterator<String> ctrys;
                
                ctrys = trans.countries(lang);                
                while( ctrys.hasNext() ) {
                    String ctry = ctrys.next();
                    String t;
                    
                    if( ctry == null || ctry.equals("") ) {
                        t = trans.getExactTranslation(new Locale(lang)).getData().toString();
                    }
                    else {
                        t = trans.getExactTranslation(new Locale(lang, ctry)).getData().toString();
                    }
                    statement.setString(OWNER_CLASS, cls);
                    statement.setString(OWNER_ID, id.toString());
                    statement.setString(ATTRIBUTE, attr);
                    statement.setString(LANGUAGE, lang);
                    if( ctry == null || ctry.equals("") ) {
                        statement.setNull(COUNTRY, Types.VARCHAR);
                    }
                    else {
                        statement.setString(COUNTRY, ctry);
                    }
                    statement.setString(TRANSLATION, t);
                    statement.executeUpdate();
                    statement.clearParameters();
                }
            }
            else {
                String t = trans.getExactTranslation(null).getData().toString();
                
                statement.setString(OWNER_CLASS, cls);
                statement.setString(OWNER_ID, id.toString());
                statement.setString(ATTRIBUTE, attr);
                statement.setNull(LANGUAGE, Types.VARCHAR);
                statement.setNull(COUNTRY, Types.VARCHAR);
                statement.setString(TRANSLATION, t);
                statement.executeUpdate();
                statement.clearParameters();
            }
        }
        return null;
    }
}

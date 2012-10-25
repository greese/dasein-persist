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

/* $Id */
/* Copyright (c) 2005-2007 Valtira LLC, All Rights Reserved */
package org.dasein.persist.jdbc;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;
import org.dasein.util.Translator;

/**
 * <p>
 *   Saves translations to the database.
 * </p>
 * <p>
 *   Last modified: $Date: 2007/08/17 16:47:41 $
 * </p>
 * @version $Revision: 1.3 $
 * @author George Reese
 */
public abstract class TranslatorUpdater extends TranslationSql {
    static public final int OWNER_ID    = 1;
    static public final int ATTRIBUTE   = 2;
    static public final int LANGUAGE    = 3;
    static public final int COUNTRY     = 4;
    static public final int TRANSLATION = 5;
    
    private String sql = null;
    
    public String getStatement() throws SQLException {
        if( sql == null ) {
            StringBuilder str = new StringBuilder();
            String tbl = getTable();
            
            str.append("INSERT INTO ");
            str.append(getIdentifier(tbl));
            str.append(" ( ");
            str.append(getIdentifier("owner_id"));
            str.append(", ");
            str.append(getIdentifier("attribute"));
            str.append(", ");
            str.append(getIdentifier("language"));
            str.append(", ");
            str.append(getIdentifier("country"));
            str.append(", ");
            str.append(getIdentifier("translation"));
            str.append(" ) VALUES ( ?, ?, ?, ?, ? )");
            if( getConnection() == null ) {
                return str.toString();
            }
            sql = str.toString();
        }
        return sql;
    }
    
    public abstract String getTable();

    public Map<String, Object> run(Transaction xaction, Map<String,Object> params) throws PersistenceException, SQLException {
        Translator<?> trans = (Translator<?>)params.get("translation");
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

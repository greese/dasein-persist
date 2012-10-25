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

/* $Id: RemoveTranslator.java,v 1.1 2005/09/14 14:18:40 greese Exp $ */
/* Copyright (c) 2005 Valtira Corporation, All Rights Reserved */
package org.dasein.persist.dao;

import java.sql.SQLException;
import java.util.Map;

import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;

/**
 * <p>
 *   LoadTranslator
 *   TODO Document this class.
 * </p>
 * <p>
 *   Last modified: $Date: 2005/09/14 14:18:40 $
 * </p>
 * @version $Revision: 1.1 $
 * @author george
 */
public class RemoveTranslator extends Execution {
    static public final int W_OWNER_CLASS = 1;
    static public final int W_OWNER_ID    = 2;
    
    private String sql = null;
    
    public String getStatement() throws SQLException {
        if( sql == null ) {
            StringBuilder str = new StringBuilder();
            String tbl = getTable();
            
            str.append("DELETE FROM ");
            str.append(getIdentifier(tbl));
            str.append(" WHERE ");
            str.append(getIdentifier("owner_class"));
            str.append(" = ? AND ");
            str.append(getIdentifier("owner_id"));
            str.append("  = ?");
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
    
    public Map<String, Object> run(Transaction xaction, Map<String,Object> state) throws PersistenceException, SQLException {
        Class cls = (Class)state.get("ownerClass");
        Object id = state.get("ownerId");

        statement.setString(W_OWNER_CLASS, cls.getName());
        statement.setString(W_OWNER_ID, id.toString());
        statement.executeUpdate();
        return null;
    }
}

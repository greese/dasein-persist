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

import org.dasein.persist.jdbc.AutomatedSql.Operator;
import org.dasein.util.CachedItem;

public class SearchTerm {
    private String                      column;
    private Class<? extends CachedItem> joinEntity;
    private Operator                    operator;
    private Object                      value;
    
    public SearchTerm(String column, Object value) {
        this(null, column, Operator.EQUALS, value);
    }
    
    public SearchTerm(String column, Operator operator, Object value) {
        this(null, column, operator, value);
    }
    
    public SearchTerm(Class<? extends CachedItem> entity, String column, Operator operator, Object value) {
        this.joinEntity = entity;
        this.column = column;
        this.operator = operator;
        this.value = value;
    }
    
    public String getColumn() {
        return column;
    }
    
    public Class<? extends CachedItem> getJoinEntity() {
        return joinEntity;
    }
    
    public Operator getOperator() {
        return operator;
    }
    
    public Object getValue() {
        return value;
    }
    
    public String toString() {
        String cname;
        
        if( joinEntity == null ) {
            cname = "";
        }
        else {
            int idx;
            
            cname = joinEntity.getClass().getName();
            idx = cname.lastIndexOf('.');
            if( idx > -1 ) {
                cname = cname.substring(idx+1);
            }
            cname = cname + ".";
        }
        return (cname + column + " " + operator + " " + value);
    }
}

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

/* $Id: PersistenceException.java,v 1.1 2005/06/09 17:44:57 george Exp $ */
/* Copyright (c) 2002-2004 Valtira Corporation, All Rights Reserved */
package org.dasein.persist;

// Developed by George Reese for the book:
// Java Best Practices, Volume II: J2EE
// Ported to the digital@jwt code library by George Reese

/**
 * Represents an error accessing some sort of data store. This
 * class generally encapsulates an underlying data source exception
 * explaining the true nature of the error.
 * <br/>
 * Last modified $Date: 2005/06/09 17:44:57 $
 * @version $Revision: 1.1 $
 * @author George Reese
 */
public class PersistenceException extends Exception {
    /**
     * Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 3546084640026080567L;
    /**
     * The exception that led to this persistence problem.
     */
    private Exception cause = null;

    /**
     * Constructs a new persistence exception that appears to
     * have happened for new good readon whatsoever.
     */
    public PersistenceException() {
        super();
    }

    /**
     * Constructs a new persistence exception with the specified
     * explanation.
     * @param msg the explanation for the error
     */
    public PersistenceException(String msg) {
        super(msg);
    }


    /**
     * Constructs a new persistence exception that results from the
     * specified data store exception.
     * @param cse the cause for this persistence exception
     */
    public PersistenceException(Exception cse) {
        super(cse.getMessage(), cse);
        cause = cse;
    }

    /**
     * @return the cause of this exception
     */
    public Exception getRootCause() {
        return cause;
    }
}

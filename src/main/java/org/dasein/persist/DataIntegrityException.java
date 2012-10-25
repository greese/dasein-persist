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

/**
 * <p>
 * Thrown when some database request would result in violating integrity constraints.
 * For example, an application that uses a last modified date to manage concurrent
 * data management might throw this exception if the timestamp has been altered.
 * </p>
 * <p>
 *   Last modified: $Date: 2005/08/15 16:15:59 $
 * </p>
 * @version $Revision: 1.3 $
 * @author George Reese
 */
public class DataIntegrityException extends PersistenceException {
    /**
     * <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 3257285816496765232L;

    /**
     * Constructs a new exception without explanation.
     */
    public DataIntegrityException() {
        super();
    }

    /**
     * Constructs a new exception with the specified error message.
     * @param msg a message indicating what happened.
     */
    public DataIntegrityException(String msg) {
        super(msg);
    }
}

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

/* $Id: ConnectionMonitor.java,v 1.3 2006/08/31 19:04:07 greese Exp $ */
/* Copyright (c) 2006 Valtira Corporation, All Rights Reserved */
package org.dasein.persist;

import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class ConnectionMonitor implements ServletContextListener {

    private static final Logger logger = Logger.getLogger(ConnectionMonitor.class);

    private boolean running = false;
    
    public void contextInitialized(ServletContextEvent event) {
        Thread t = new Thread() {
            public void run() {
                synchronized( this ) {
                    running = true;
                    while( running ) {
                        try { wait(60000L); }
                        catch( InterruptedException ignore ) { /* ignore */ }
                        report();
                    }
                }
                logger.info("Dasein connection monitor is shut down.");
            }
        };
        logger.info("The Dasein connection monitor is installed and will report every minute.");
        logger.info("To turn off the Dasein connection monitor, remove the listener entry from your web.xml.");
        t.setDaemon(false);
        t.setName("DASEIN CONNECTION MONITOR");
        t.start();
    }
    
    public void contextDestroyed(ServletContextEvent event) {
        logger.info("Shutting down the Dasein connection monitor.");
        synchronized( this ) {
            running = false;
            notifyAll();
        }
    }
    
    public void report() {
        Transaction.report();
    }
}

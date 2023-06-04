package org.frameworkset.upgrade;
/**
 * Copyright 2023 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.frameworkset.common.poolman.util.DBConf;
import org.frameworkset.tran.upgrade.UpgradeBboss;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/6/4
 * @author biaoping.yin
 * @version 1.0
 */
public class StatusUpgrade {
    public static void main(String[] args){
        UpgradeBboss upgradeBboss = new UpgradeBboss();
        DBConf tempConf = new DBConf();
        tempConf.setPoolname("upgrade");
        tempConf.setDriver("org.sqlite.JDBC");
        tempConf.setJdbcurl("jdbc:sqlite://E:\\workspace\\bbossgroups\\bboss-elastic\\StatusStoreDB");
        tempConf.setUsername("root");
        tempConf.setPassword("Root_123456#");
        tempConf.setReadOnly((String)null);
        tempConf.setTxIsolationLevel((String)null);
        tempConf.setValidationQuery("select 1");
        tempConf.setJndiName("upgrade-jndi");
        tempConf.setInitialConnections(1);
        tempConf.setMinimumSize(1);
        tempConf.setMaximumSize(1);
        tempConf.setUsepool(true);
        tempConf.setExternal(false);
        tempConf.setExternaljndiName((String)null);
        tempConf.setShowsql(false);
        tempConf.setEncryptdbinfo(false);
        tempConf.setQueryfetchsize(null);
        upgradeBboss.upgradeStatus(tempConf,"increament_tab");
    }
}

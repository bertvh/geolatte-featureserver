/*
 * This file is part of the GeoLatte project. This code is licenced under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 *
 * Copyright (C) 2010 - 2010 and Ownership of code is shared by:
 * Qmino bvba - Romeinsestraat 18 - 3001 Heverlee  (http://www.Qmino.com)
 * Geovise bvba - Generaal Eisenhowerlei 9 - 2140 Antwerpen (http://www.geovise.com)
 */

package org.geolatte.featureserver;

import org.geolatte.featureserver.config.FeatureServerConfiguration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * No comment provided yet for this class.
 * <p/>
 * <p>
 * <i>Creation-Date</i>: 1-jul-2010<br>
 * <i>Creation-Time</i>: 14:19:00<br>
 * </p>
 *
 * @author Yves Vandewoude
 * @author <a href="http://www.qmino.com">Qmino bvba</a>
 * @since SDK1.5
 */
public class MappingConfigurationTest {

//    @Test
//    public void testParseConstructor()
//    {
//        FeatureServerConfiguration mc = FeatureServerConfiguration.getInstance();
//        List<String> tables = new ArrayList<String>();
//        tables.add("BertTest");
//        tables.add("Yves");
//        tables.add("TBL_Test");
//        tables.add("VIEW_POJO");
//        List<String> accepted = mc.includedTables(tables);
//        for (String s: accepted)
//        {
//            System.out.println(s);
//        }
//    }

    @Test
    public void testReadConfigFileFromSysProperty() {
        System.setProperty("geolatte.fs.config", "src/test/resources/test-config.xml");
        FeatureServerConfiguration cfg = FeatureServerConfiguration.getInstance();


    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dist.kvstore;

import java.math.BigInteger;
import java.util.Comparator;

/**
 * This class generates a MD5 hash of the key. It uses the standard technique
 * used in all DHT's.
 * 
 * @author alakshman
 * 
 */
public class RandomPartitioner implements IPartitioner
{
    private static final Comparator<String> comparator = new Comparator<String>() {
        public int compare(String o1, String o2)
        {
            BigInteger i1 = new BigInteger(o1.split(":")[0]);
            BigInteger i2 = new BigInteger(o2.split(":")[0]);
            return i2.compareTo(i1);
        }
    };

    public BigInteger hash(String key)
	{
		return FBUtilities.hash(key);
	}

    public String decorateKey(String key)
    {
        return hash(key).toString() + ":" + key;
    }

    public String undecorateKey(String decoratedKey)
    {
        return decoratedKey.split(":")[1];
    }

    public Comparator<String> getReverseDecoratedKeyComparator()
    {
        return comparator;
    }
}
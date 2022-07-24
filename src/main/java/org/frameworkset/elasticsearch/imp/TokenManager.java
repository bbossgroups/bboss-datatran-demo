package org.frameworkset.elasticsearch.imp;
/**
 * Copyright 2022 bboss
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

import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.util.TimeUtil;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/24
 * @author biaoping.yin
 * @version 1.0
 */
public class TokenManager {
	private TokenInfo tokenInfo;

	/**
	 * 如果token不存在或者token过期，则调用jwtservice /api/auth/v1.0/getToken申请token
	 * @return
	 */
	public synchronized TokenInfo getTokenInfo(){
		if(tokenInfo == null || expired()){//没有token或者token过期
			Map params = new LinkedHashMap();
			/**
			 *  "device_id": "app03001",
			 *     "app_id": "app03",
			 *     "signature": "1b3bb71f6ebae2f52b7a238c589f3ff9",
			 *     "uuid": "adfdafadfadsfe34132fdsadsfadsf"
			 */
			params.put("device_id","app03001");
			params.put("app_id","app03");
			params.put("signature","1b3bb71f6ebae2f52b7a238c589f3ff9");
			params.put("uuid","adfdafadfadsfe34132fdsadsfadsf");
			Map datas = HttpRequestProxy.sendJsonBody("jwtservice",params,"/api/auth/v1.0/getToken", Map.class);
			if(datas != null){
				int code = (int)datas.get("code");
				if(code == 200) {
					Map<String, Object> tokens = (Map<String, Object>) datas.get("data");
					TokenInfo tokenInfo = new TokenInfo();
					tokenInfo.setTokenTimestamp(new Date());
					tokenInfo.setAccess_token((String)tokens.get("access_toke"));
					tokenInfo.setExpires_time((int)tokens.get("expires_time"));
					tokenInfo.setExpiredTimestamp(TimeUtil.addDateSeconds(tokenInfo.getTokenTimestamp(),tokenInfo.getExpires_time()));
					this.tokenInfo = tokenInfo;
				}


			}
			if(tokenInfo == null){
				throw new DataImportException("get token failed: token info is null");
			}
			return tokenInfo;
		}
		else{
			return tokenInfo;
		}
	}

	private boolean expired(){
		return tokenInfo.getExpiredTimestamp().before(new Date());
	}
}

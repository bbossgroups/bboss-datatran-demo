package org.frameworkset.datatran.imp;
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

import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/24
 * @author biaoping.yin
 * @version 1.0
 */
public class TokenInfo {
	private String access_token;
	private Date tokenTimestamp;
	private Date expiredTimestamp;
	private int expires_time;

	public Date getExpiredTimestamp() {
		return expiredTimestamp;
	}

	public void setExpiredTimestamp(Date expiredTimestamp) {
		this.expiredTimestamp = expiredTimestamp;
	}

	public String getAccess_token() {
		return access_token;
	}

	public void setAccess_token(String access_token) {
		this.access_token = access_token;
	}

	public Date getTokenTimestamp() {
		return tokenTimestamp;
	}

	public void setTokenTimestamp(Date tokenTimestamp) {
		this.tokenTimestamp = tokenTimestamp;
	}

	public int getExpires_time() {
		return expires_time;
	}

	public void setExpires_time(int expires_time) {
		this.expires_time = expires_time;
	}
}

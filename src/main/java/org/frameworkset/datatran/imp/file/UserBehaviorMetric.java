package org.frameworkset.datatran.imp.file;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.TimeMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * <p>Description: 用户行为统计</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2025/8/20 10:34
 * @author
 * @version 1.0
 */
public class UserBehaviorMetric extends TimeMetric {

	private static Logger logger = LoggerFactory.getLogger(UserBehaviorMetric.class);

	/**
	 * 创建时间
	 */
	private Date createTime;

	/**
	 * 手机号码
	 */
	private String mobile;
	/**
	 * 活跃等级（从0-5，0代表不活跃，5代表一天内5次及以上登录访问宽带）
	 */
	private int activeLevel;
	/**
	 * 搜索宽带相关记录
	 */
	private String qryKdMsg;
	/**
	 * 修改时间
	 */
	private Date updateTime;



	public void init(MapData firstData) {
		CommonRecord record = ((CommonRecord) firstData.getData());
		createTime = (Date) record.getData("CREATE_TIME");
		mobile = (String) record.getData("USER");
		updateTime = (Date) record.getData("UPDATE_TIME");
	}

	public void incr(MapData data) {
		CommonRecord record = ((CommonRecord) data.getData());
		String eventKey = (String) record.getData("EVENT_KEY");
		if("$page".equals(eventKey)) {
			activeLevel++;
//			if(activeLevel > 5){
//				activeLevel = 5;
//			}
		} else if("xcxsearchEventClick".equals(eventKey)){
			String searchWordsVar = (String) record.getData("searchWords_var");
			if(qryKdMsg == null) {
				qryKdMsg = searchWordsVar;
//				logger.info("用户搜索宽带相关记录：" + qryKdMsg);
			} else {
				if(qryKdMsg.contains(searchWordsVar)) {
					return;
				}
				qryKdMsg += ";" + searchWordsVar;
			}
		} else if("imp".equals(eventKey)) {
			String xyScene = (String) record.getData("XY_SCENE");
			if(qryKdMsg == null) {
				qryKdMsg = xyScene;
//				logger.info("用户搜索宽带相关记录：" + qryKdMsg);
			} else {
				if(qryKdMsg.contains(xyScene)) {
					return;
				}
				qryKdMsg += ";" + xyScene;
			}
		}
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public int getActiveLevel() {
		return activeLevel;
	}

	public void setActiveLevel(int activeLevel) {
		this.activeLevel = activeLevel;
	}

	public String getQryKdMsg() {
		return qryKdMsg;
	}

	public void setQryKdMsg(String qryKdMsg) {
		this.qryKdMsg = qryKdMsg;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}
}

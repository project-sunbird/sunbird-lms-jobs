package org.sunbird.jobs.samza.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.samza.config.Config;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.jobs.samza.util.JSONUtils;
import org.sunbird.jobs.samza.util.JobLogger;
import org.sunbird.notification.beans.SMSConfig;
import org.sunbird.notification.fcm.provider.IFCMNotificationService;
import org.sunbird.notification.fcm.provider.NotificationFactory;
import org.sunbird.notification.fcm.providerImpl.FCMHttpNotificationServiceImpl;
import org.sunbird.notification.sms.provider.ISmsProvider;
import org.sunbird.notification.utils.FCMResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.sunbird.jobs.samza.util.Constant;
import org.sunbird.notification.utils.SMSFactory;

/**
 * 
 * @author manzarul
 *
 */
public class NotificationService {
	ObjectMapper mapper = new ObjectMapper();
	private JobLogger Logger = new JobLogger(NotificationService.class);
	private Config appConfig = null;
	private String accountKey = null;
	private String msgAuthKey = null;
	private ISmsProvider smsProvider = null;
	private String[] mandatoryParams = null;
	private IFCMNotificationService ifcmNotificationService = NotificationFactory
			.getInstance(NotificationFactory.instanceType.httpClinet.name());
	private static final String FCM_ACCOUNT_KEY = "fcm_account_key";
	private static final String MSG_AUTH_KEY = "msg_auth_key";

	public void initialize(Config config) throws Exception {
		JSONUtils.loadProperties(config);
		appConfig = config;
		mandatoryParams = new String[]{
						Constant.ACTION_NAME, Constant.IDS, Constant.EDATA
		};
		accountKey = appConfig.get(FCM_ACCOUNT_KEY);
		msgAuthKey = appConfig.get(MSG_AUTH_KEY);
		SMSConfig smsConfig = new SMSConfig(msgAuthKey, "");
		smsProvider = SMSFactory.getInstance("91SMS", smsConfig);
		Logger.info("NotificationService:initialize: Service config initialized");
	}

	public void processMessage(Map<String, Object> message) throws Exception {
		Logger.info("Account key:"+ accountKey);
		FCMHttpNotificationServiceImpl.setAccountKey(accountKey);
		String msgId = (String) message.get(Constant.MID);
		Map<String, Object> validationMap = null;
		Map<String, String> dataMap = new HashMap<String, String>();
		Map<String, Object> edataMap = (Map<String, Object>) message.get(Constant.EDATA);
		Map<String, Object> objectMap = (Map<String, Object>) message.get(Constant.OBJECT);
		String requestHash = "";
		boolean isSuccess = false;
		if (edataMap != null && edataMap.size() > 0) {
			String actionValue = (String) edataMap.get(Constant.ACTION);
			validationMap.put(Constant.ACTION, actionValue);
			if (Constant.ACTION_NAME.equalsIgnoreCase(actionValue)) {
				Map<String, Object> requestMap = (Map<String, Object>) edataMap.get(Constant.REQUEST);
				requestHash = OneWayHashing.encryptVal(mapper.writeValueAsString(requestMap));
				if (!requestHash.equals((String) objectMap.get(Constant.ID))) {
					Logger.info("NotificationService:processMessage: hashValue is not matching - " + requestHash);
				} else {
					Map<String, Object> notificationMap = (Map<String, Object>) requestMap.get(Constant.NOTIFICATION);
					if(notificationMap.get(Constant.MODE).equals("phone")) {
						isSuccess = sendSmsNotification(notificationMap, msgId);
					} else if(notificationMap.get(Constant.MODE).equals("device")){
						isSuccess = notifyDevice(notificationMap);
					}
					if (isSuccess) {
						Logger.info("Notification sent to device successfully.");
					} else {
						Logger.info("Notification sent failure");
					}
				}
			} else {
				Logger.info("NotificationService:processMessage action name is incorrect: " + actionValue);
			}
		} else {
			Logger.info("NotificationService:processMessage event data map is either null or empty");
		}
	}

	private boolean sendSmsNotification(Map<String, Object> notificationMap, String msgId) {
		List<String> deviceIds = (List<String>) notificationMap.get(Constant.IDS);
		if (deviceIds != null) {
			Map<String, Object> templateMap = (Map<String, Object>) notificationMap.get(Constant.TEMPLATE);
			String smsText = (String) templateMap.get(Constant.DATA);
			return smsProvider.bulkSms(deviceIds, smsText);
		} else {
			Logger.info("mobile numbers not provided for message id:"+msgId);
			return true;
		}
	}

	private boolean notifyDevice(Map<String, Object> notificationMap) throws JsonProcessingException {
		String topic = null;
		FCMResponse response = null;
		List<String> deviceIds = (List<String>) notificationMap.get(Constant.IDS);
		Map<String, String> dataMap = new HashMap<String, String>();
		dataMap.put(Constant.RAW_DATA, mapper.writeValueAsString(notificationMap.get(Constant.RAW_DATA)));
		Logger.info("NotificationService:processMessage: calling send notification ");
		if (deviceIds != null ) {
			//return batchNotifyDevice(deviceIds, dataMap);
			response = ifcmNotificationService.sendMultiDeviceNotification(deviceIds, dataMap, false);
		} else {
			Map<String, Object> configMap = (Map<String, Object>) notificationMap.get(Constant.CONFIG);
			topic = (String) configMap.getOrDefault(Constant.TOPIC, "");
			response = ifcmNotificationService.sendTopicNotification(topic, dataMap, false);
		}
		if (response != null) {
			Logger.info("Send device notiifcation response with canonicalId,ErrorMsg,successCount,FailureCount"
							+ response.getCanonical_ids() + "," + response.getError() + ", " + response.getSuccess() + " "
							+ response.getFailure());
			return true;
		} else {
			Logger.info("response is improper from fcm:" + response + "for device ids" + deviceIds + "or topic"+ topic);
			return false;
		}
	}

}

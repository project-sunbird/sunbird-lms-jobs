package org.sunbird.jobs.samza.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private IFCMNotificationService ifcmNotificationService = NotificationFactory
			.getInstance(NotificationFactory.instanceType.httpClinet.name());
	private static final String FCM_ACCOUNT_KEY = "fcm_account_key";
	private static final String MSG_AUTH_KEY = "msg_auth_key";

	public void initialize(Config config) throws Exception {
		JSONUtils.loadProperties(config);
		appConfig = config;
		Logger.info("NotificationService:initialize: Service config initialized");
		accountKey = appConfig.get(FCM_ACCOUNT_KEY);
		msgAuthKey = appConfig.get(MSG_AUTH_KEY);
		SMSConfig smsConfig = new SMSConfig(msgAuthKey, "");
		smsProvider = SMSFactory.getInstance("91SMS", smsConfig);
	}

	public void processMessage(Map<String, Object> message) throws Exception {
		Logger.info("Account key:"+ accountKey);
		FCMHttpNotificationServiceImpl.setAccountKey(accountKey);
		Map<String, String> notificationMap = new HashMap<String, String>();
		Map<String, Object> edataMap = (Map<String, Object>) message.get(Constant.EDATA);
		String requestHash = "";
		List<String> deviceIds = null;
		String topic = null;
		boolean isSuccess = false;
		if (edataMap != null && edataMap.size() > 0) {
			String actionValue = (String) edataMap.get(Constant.ACTION);
			if (Constant.ACTION_NAME.equalsIgnoreCase(actionValue)) {
				Map<String, Object> requestMap = (Map<String, Object>) edataMap.get(Constant.REQUEST);
				requestHash = OneWayHashing.encryptVal(mapper.writeValueAsString(requestMap));
				Map<String, Object> objectMap = (Map<String, Object>) message.get(Constant.OBJECT);
				if (!requestHash.equals((String) objectMap.get(Constant.ID))) {
					Logger.info("NotificationService:processMessage: hashValue is not matching - " + requestHash);
				} else {
					Map<String, Object> tmp = (Map<String, Object>) requestMap.get(Constant.NOTIFICATION);
					if (tmp.get(Constant.IDS) != null) {
						deviceIds = (List<String>) tmp.get(Constant.IDS);
					}
					if(tmp.get(Constant.MODE).equals("phone")) {
						if(deviceIds != null) {
							Map<String, Object> templateMap = (Map<String, Object>) tmp.get(Constant.TEMPLATE);
							String smsData = (String) templateMap.get(Constant.DATA);
							isSuccess = sendSmsNotification(deviceIds, smsData);
						} else {
							Logger.info("mobile numbers not provided");
						}
					} else {
						if (deviceIds == null) {
							Map<String, Object> configMap = (Map<String, Object>) tmp.get(Constant.CONFIG);
							topic = (String) configMap.getOrDefault(Constant.TOPIC, "");
						}
						notificationMap.put(Constant.RAW_DATA, mapper.writeValueAsString(tmp.get(Constant.RAW_DATA)));
						Logger.info("NotificationService:processMessage: calling send notification ");
						isSuccess = notifyDevice(notificationMap, deviceIds, topic);
					}
					if (isSuccess) {
						Logger.info("Notification sent to device successfully.");
					} else {
						Logger.info("Notification sent failure for device or token " + deviceIds + " _" + topic);
					}
				}
			} else {
				Logger.info("NotificationService:processMessage procide actioname is incorrect: " + actionValue);
			}
		} else {
			Logger.info("NotificationService:processMessage event data map is either null or empty");
		}
	}

	private boolean sendSmsNotification(List<String> deviceIds, String smsText) {
		return smsProvider.bulkSms(deviceIds, smsText);
	}

	private boolean notifyDevice(Map<String, String> notificationMap, List<String> deviceIds, String topic) {
		if (deviceIds != null && deviceIds.size() > 0) {
			if (deviceIds.size() <= 100) {
				return batchNotifyDevice(deviceIds, notificationMap);
			} else {
				List<String> tmp = new ArrayList<String>();
				for (int i = 0; i < deviceIds.size(); i++) {
					tmp.add(deviceIds.get(i));
					if (tmp.size() == 100 || i == (deviceIds.size() - 1)) {
						batchNotifyDevice(tmp, notificationMap);
						tmp.clear();
					}
				}
			}
		} else {
			FCMResponse response = ifcmNotificationService.sendTopicNotification(topic, notificationMap, false);
			Logger.info("NotificationService:notifyDevice topic based notification response :" + response.getCanonical_ids());
		}
		return true;

	}

	private boolean batchNotifyDevice(List<String> deviceIds, Map<String, String> notificationMap) {
		FCMResponse response = ifcmNotificationService.sendMultiDeviceNotification(deviceIds, notificationMap, false);
		if (response != null) {
			Logger.info("Send device notiifcation response with canonicalId,ErrorMsg,successCount,FailureCount"
					+ response.getCanonical_ids() + "," + response.getError() + ", " + response.getSuccess() + " "
					+ response.getFailure());
			return true;
		} else {
			Logger.info("response is improper from fcm:" + response);
			return false;
		}
	}

}

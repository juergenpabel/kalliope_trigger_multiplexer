#!/usr/bin/python3

import time
import logging
from threading import Thread
from kalliope import Utils
from kalliope.core.ConfigurationManager import SettingLoader
from paho.mqtt import client as mqtt_client


logging.basicConfig()
logger = logging.getLogger("kalliope")


class Multiplexer(Thread):
	def __init__(self, **kwargs):
		super(Multiplexer, self).__init__(name='trigger_multiplexer', daemon=True)
		logger.debug("[trigger:multiplexer] __init__()")
		self.config = {}
		self.config['mqtt-broker-ip'] = kwargs.get('broker_ip', '127.0.0.1')
		self.config['mqtt-broker-port'] = kwargs.get('broker_port', 1883)
		self.config['mqtt-client_id'] = kwargs.get('client_id', 'kalliope:trigger:multiplexer')
		self.config['mqtt-topic'] = kwargs.get('mqtt_topic', 'kalliope/trigger/multiplexer/event')
		self.kalliope_callback = kwargs.get('callback', None)
		if self.kalliope_callback is None:
			raise MissingParameterException("keyword parameter 'callback' is required")
		sl = SettingLoader()
		self.triggers = {}
		for trigger_name in kwargs.get('triggers', '').split(','):
			trigger_name = trigger_name.strip()
			for trigger_setting in sl.settings.triggers:
				if trigger_name == trigger_setting.name:
					logger.debug(f"[trigger:multiplexer] adding trigger '{trigger_name}'")
					trigger_setting.parameters['callback'] = self.callback
					self.triggers[trigger_name] = Utils.get_dynamic_class_instantiation(package_name="trigger",
					                                                                    module_name=trigger_name,
					                                                                    parameters=trigger_setting.parameters,
					                                                                    resources_dir=sl.settings.resources.trigger_folder)


	def run(self):
		logger.debug("[trigger:multiplexer] run()")
		self.paused = False
		for name in self.triggers.keys():
			logger.debug(f"[trigger:multiplexer] about to call {name}.run() in new thread")
			self.triggers[name].start()
		mqtt = mqtt_client.Client(self.config['mqtt-client_id'])
		mqtt.connect(self.config['mqtt-broker-ip'], self.config['mqtt-broker-port'])
		mqtt.subscribe(self.config['mqtt-topic'])
		mqtt.on_message = self.on_mqtt
		mqtt.loop_forever()


	def on_mqtt(self, client, userdata, message):
		logger.debug("[trigger:multiplexer] on_mqtt()")
		payload = message.payload.decode('utf-8')
		if payload == "pause":
			self.pause()
		if payload == "unpause":
			self.unpause()
		if payload is None or payload == "" or payload == "trigger":
			if self.paused is False:
				self.kalliope_callback()


	def pause(self):
		logger.info("[trigger:multiplexer] pause()")
		self.paused = True
		for name in self.triggers.keys():
			logger.debug(f"[trigger:multiplexer] about to call {name}.pause()")
			self.triggers[name].pause()


	def unpause(self):
		logger.info("[trigger:multiplexer] unpause()")
		self.paused = False
		for name in self.triggers.keys():
			logger.debug(f"[trigger:multiplexer] about to call {name}.unpause()")
			self.triggers[name].unpause()

	def callback(self):
		logger.info("[trigger:multiplexer] callback() [due to a callback invocation from a multiplexed trigger]")
		if self.paused is False:
			self.paused = True
			for name in self.triggers.keys():
				logger.debug(f"[trigger:multiplexer] about to call {name}.pause()")
				self.triggers[name].pause()
			self.kalliope_callback()


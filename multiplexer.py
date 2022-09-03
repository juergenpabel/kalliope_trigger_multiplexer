#!/usr/bin/python3

import time
import logging
from threading import Thread
from kalliope import Utils
from paho.mqtt import client as mqtt_client


logging.basicConfig()
logger = logging.getLogger("kalliope")


class Multiplexer(Thread):
	def __init__(self, **kwargs):
		super(Multiplexer, self).__init__(name='trigger_multiplexer', daemon=True)
		logger.debug("[trigger:multiplexer] __init__()")
		self.config = {}
		self.config['mqtt-address'] = kwargs.get('mqtt_address', 'localhost')
		self.config['mqtt-port']    = kwargs.get('mqtt_port', 1883)
		self.config['mqtt-topic-trigger'] = kwargs.get('topic_trigger', 'kalliope/trigger/multiplexer/trigger')
		self.config['mqtt-topic-pause']   = kwargs.get('topic_pause',   'kalliope/trigger/multiplexer/pause')
		self.config['mqtt-topic-unpause'] = kwargs.get('topic_unpause', 'kalliope/trigger/multiplexer/unpause')
		self.callback = kwargs.get('callback', None)
		if self.callback is None:
			raise MissingParameterException("keyword parameter 'callback' is required")
		self.triggers = {}
#TODO:parse config section, load trigger module and mash config into kwargs
#TODO		for name in triggers:
#TODO			self.triggers[name] = Trigger(name='trigger_{}'.format(name), **kwargs)


	def run(self):
		logger.debug("[trigger:multiplexer] run()")
		self.paused = False
		for name, trigger in self.triggers:
			logger.debug("[trigger:multiplexer] about to call {}.run() in new thread".format(name))
			trigger.start()
		logger.debug("[trigger:multiplexer] run()...")
		mqtt = mqtt_client.Client('kalliope_trigger_multiplexer')
		mqtt.connect(self.config['mqtt-address'], self.config['mqtt-port'])
		mqtt.subscribe(self.config['mqtt-topic-trigger'])
		mqtt.subscribe(self.config['mqtt-topic-pause'])
		mqtt.subscribe(self.config['mqtt-topic-unpause'])
		mqtt.on_message = self.on_mqtt
		mqtt.loop_forever()


	def on_mqtt(self, client, userdata, message):
		logger.debug("[trigger:multiplexer] on_mqtt()")
		if message.topic == self.config['mqtt-topic-trigger']:
			self.callback()
		if message.topic == self.config['mqtt-topic-pause']:
			self.pause()
		if message.topic == self.config['mqtt-topic-unpause']:
			self.unpause()


	def pause(self):
		logger.info("[trigger:multiplexer] pause()")
		self.paused = True
		for name, trigger in self.triggers:
			logger.debug("[trigger:multiplexer] about to call {}.pause()".format(name))
			trigger.pause()


	def unpause(self):
		logger.info("[trigger:multiplexer] unpause()")
		self.paused = False
		for name, trigger in self.triggers:
			logger.debug("[trigger:multiplexer] about to call {}.unpause()".format(name))
			trigger.unpause()


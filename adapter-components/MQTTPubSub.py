#!/usr/bin/python
# Copyright (c) 2013 Roger Light <roger@atchoo.org>
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Distribution License v1.0
# which accompanies this distribution. 
#
# The Eclipse Distribution License is available at 
#   http://www.eclipse.org/org/documents/edl-v10.php.
#
# Contributors:
#    Roger Light - initial implementation
# This example shows how you can use the MQTT client in a class.
import sys
import os
import inspect
import paho.mqtt.client as mqtt
import threading


class MQTTPubSub:
    def __init__(self, params):

        self.url = params["url"]
        self.port = params["port"]
        self.timeout = params["timeout"]
        self.topic = params["topic"]

        self._mqttc = mqtt.Client(None)
        
        if( "username" in params):
            self.username = params["username"]
            if( "password" in params):
                self.password = params["password"]
                self._mqttc.username_pw_set(self.username,self.password)

        if ("onMessage" in params):
            self._mqttc.on_message = params["onMessage"]
        if ("onConnect" in params):
            self._mqttc.on_connect = params["onConnect"]
        if ("onPublish" in params):
            self._mqttc.on_publish = params["onPublish"]
        if ("onSubscribe" in params):
            self._mqttc.on_subscribe = params["onSubscribe"]
        if ("onDisconnect" in params):
            self._mqttc.on_disconnect = params["onDisconnect"]


    def publish(self, payload):
        self._mqttc.publish(self.topic, payload)


    def run(self):
        self._mqttc.connect(self.url, self.port, self.timeout)
        self._mqttc.subscribe(self.topic, 0)
        threading.Thread(target=self._mqttc.loop_start()).start()
        







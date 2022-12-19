#!/usr/bin/env python

import os
import sys
from script_utils import SCRIPT_HOME, VERSION
sys.path.insert(1, os.path.join(os.path.dirname(__file__), f"{SCRIPT_HOME}/ext"))

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GLib
import logging
from vedbus import VeDbusService
from dbusmonitor import DbusMonitor
from collections import namedtuple

DEVICE_INSTANCE_ID = 1024
PRODUCT_ID = 0
PRODUCT_NAME = "DC System Aggregator"
FIRMWARE_VERSION = 0
HARDWARE_VERSION = 0
CONNECTED = 1
ALARM_OK = 0

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dcsystem")


class SystemBus(dbus.bus.BusConnection):
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)


class SessionBus(dbus.bus.BusConnection):
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)


def dbusConnection():
    return SessionBus() if 'DBUS_SESSION_BUS_ADDRESS' in os.environ else SystemBus()


DCService = namedtuple('DCService', ['name', 'type'])


class DCSystemService:
    def __init__(self, conn):
        self.service = VeDbusService('com.victronenergy.dcsystem.aggregator', conn)
        self.service.add_mandatory_paths(__file__, VERSION, 'dbus', DEVICE_INSTANCE_ID,
                                     PRODUCT_ID, PRODUCT_NAME, FIRMWARE_VERSION, HARDWARE_VERSION, CONNECTED)
        self.service.add_path("/Dc/0/Voltage", 0)
        self.service.add_path("/Dc/0/Current", 0)
        self.service.add_path("/History/EnergyIn", 0)
        self.service.add_path("/History/EnergyOut", 0)
        self.service.add_path("/Alarms/LowVoltage", ALARM_OK)
        self.service.add_path("/Alarms/HighVoltage", ALARM_OK)
        self.service.add_path("/Alarms/LowTemperature", ALARM_OK)
        self.service.add_path("/Alarms/HighTemperature", ALARM_OK)
        self.service.add_path("/Dc/0/Power", 0)
        options = None  # currently not used afaik
        self.monitor = DbusMonitor({
            'com.victronenergy.dcload': {
                '/Dc/0/Current': options,
                '/Dc/0/Voltage': options,
                '/History/EnergyIn': options,
                '/Alarms/LowVoltage': options,
                '/Alarms/HighVoltage': options,
                '/Alarms/LowTemperature': options,
                '/Alarms/HighTemperature': options,
                '/Dc/0/Power': options
            },
            'com.victronenergy.dcsource': {
                '/Dc/0/Current': options,
                '/Dc/0/Voltage': options,
                '/History/EnergyOut': options,
                '/Alarms/LowVoltage': options,
                '/Alarms/HighVoltage': options,
                '/Alarms/LowTemperature': options,
                '/Alarms/HighTemperature': options,
                '/Dc/0/Power': options
            }
        })

    def _get_value(self, serviceName, path, defaultValue=None):
        return self.monitor.get_value(serviceName, path, defaultValue)

    def update(self):
        totalCurrent = 0
        totalPower = 0
        totalEnergyIn = 0
        totalEnergyOut = 0
        maxLowVoltageAlarm = ALARM_OK
        maxHighVoltageAlarm = ALARM_OK
        maxLowTempAlarm = ALARM_OK
        maxHighTempAlarm = ALARM_OK

        dcServices = []
        for serviceType in ['dcload', 'dcsource']:
            for serviceName in self.monitor.get_service_list('com.victronenergy.' + serviceType):
                dcServices.append(DCService(serviceName, serviceType))

        for dcService in dcServices:
            serviceName = dcService.name
            current = self._get_value(serviceName, "/Dc/0/Current", 0)
            voltage = self._get_value(serviceName, "/Dc/0/Voltage", 0)
            if dcService.type == 'dcload':
                totalEnergyIn += self._get_value(serviceName, "/History/EnergyIn", 0)
            else:
                current -= current
                totalEnergyOut += self._get_value(serviceName, "/History/EnergyOut", 0)
            totalCurrent += current
            totalPower += voltage * current

            maxLowVoltageAlarm = max(self._get_value(serviceName, "/Alarms/LowVoltage", ALARM_OK), maxLowVoltageAlarm)
            maxHighVoltageAlarm = max(self._get_value(serviceName, "/Alarms/HighVoltage", ALARM_OK), maxHighVoltageAlarm)
            maxLowTempAlarm = max(self._get_value(serviceName, "/Alarms/LowTemperature", ALARM_OK), maxLowTempAlarm)
            maxHighTempAlarm = max(self._get_value(serviceName, "/Alarms/HighTemperature", ALARM_OK), maxHighTempAlarm)

        self.service["/Dc/0/Voltage"] = round(totalPower/totalCurrent, 3) if totalCurrent else 0
        self.service["/Dc/0/Current"] = round(totalCurrent, 3)
        self.service["/History/EnergyIn"] = round(totalEnergyIn, 6)
        self.service["/History/EnergyOut"] = round(totalEnergyOut, 6)
        self.service["/Alarms/LowVoltage"] = maxLowVoltageAlarm
        self.service["/Alarms/HighVoltage"] = maxHighVoltageAlarm
        self.service["/Alarms/LowTemperature"] = maxLowTempAlarm
        self.service["/Alarms/HighTemperature"] = maxHighTempAlarm
        self.service["/Dc/0/Power"] = round(totalPower, 3)
        return True

    def __str__(self):
        return "DC System Aggregator"


def main():
    DBusGMainLoop(set_as_default=True)
    dcSystem = DCSystemService(dbusConnection())
    GLib.timeout_add(1000, dcSystem.update)
    logger.info("Registered DC System Aggregator")
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()

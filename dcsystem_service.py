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
from settableservice import SettableService
from dbusmonitor import DbusMonitor
from collections import namedtuple

DEVICE_INSTANCE_ID = 1024
PRODUCT_ID = 0
PRODUCT_NAME = "DC System Aggregator"
FIRMWARE_VERSION = 0
HARDWARE_VERSION = 0
CONNECTED = 1

ALARM_OK = 0
ALARM_WARNING = 1
ALARM_ALARM = 2

VOLTAGE_DEADBAND = 1.0

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


VOLTAGE_TEXT = lambda path,value: "{:.2f}V".format(value)
CURRENT_TEXT = lambda path,value: "{:.3f}A".format(value)
POWER_TEXT = lambda path,value: "{:.2f}W".format(value)
ENERGY_TEXT = lambda path,value: "{:.6f}kWh".format(value)


class DCSystemService(SettableService):
    def __init__(self, conn):
        super().__init__()
        self.service = VeDbusService('com.victronenergy.dcsystem.aggregator', conn, register=False)
        self.add_settable_path("/CustomName", "")
        self._init_settings(conn)
        di = self.register_device_instance("dcsystem", "DCSystemAggregator", DEVICE_INSTANCE_ID)
        self.service.add_mandatory_paths(__file__, VERSION, 'dbus', di,
                                     PRODUCT_ID, PRODUCT_NAME, FIRMWARE_VERSION, HARDWARE_VERSION, CONNECTED)
        self.service.add_path("/Dc/0/Voltage", 0, gettextcallback=VOLTAGE_TEXT)
        self.service.add_path("/Dc/0/Current", 0, gettextcallback=CURRENT_TEXT)
        self.service.add_path("/History/EnergyIn", 0, gettextcallback=ENERGY_TEXT)
        self.service.add_path("/History/EnergyOut", 0, gettextcallback=ENERGY_TEXT)
        self.service.add_path("/Alarms/LowVoltage", ALARM_OK)
        self.service.add_path("/Alarms/HighVoltage", ALARM_OK)
        self.service.add_path("/Alarms/LowTemperature", ALARM_OK)
        self.service.add_path("/Alarms/HighTemperature", ALARM_OK)
        self.service.add_path("/Dc/0/Power", 0, gettextcallback=POWER_TEXT)
        self.service.register()
        self._local_values = {}
        for path in self.service._dbusobjects:
            self._local_values[path] = self.service[path]
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
        voltageSum = 0
        voltageCount = 0
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
            power = self._get_value(serviceName, "/Dc/0/Power", voltage * current)
            if dcService.type == 'dcload':
                totalEnergyIn += self._get_value(serviceName, "/History/EnergyIn", 0)
            else:
                current = -current
                power = -power
                totalEnergyOut += self._get_value(serviceName, "/History/EnergyOut", 0)
            totalCurrent += current
            if voltage > VOLTAGE_DEADBAND:
                voltageSum += voltage
                voltageCount += 1
            totalPower += power

            maxLowVoltageAlarm = max(self._get_value(serviceName, "/Alarms/LowVoltage", ALARM_OK), maxLowVoltageAlarm)
            maxHighVoltageAlarm = max(self._get_value(serviceName, "/Alarms/HighVoltage", ALARM_OK), maxHighVoltageAlarm)
            maxLowTempAlarm = max(self._get_value(serviceName, "/Alarms/LowTemperature", ALARM_OK), maxLowTempAlarm)
            maxHighTempAlarm = max(self._get_value(serviceName, "/Alarms/HighTemperature", ALARM_OK), maxHighTempAlarm)

        self._local_values["/Dc/0/Voltage"] = voltageSum/voltageCount if voltageCount > 0 else 0
        self._local_values["/Dc/0/Current"] = totalCurrent
        self._local_values["/History/EnergyIn"] = totalEnergyIn
        self._local_values["/History/EnergyOut"] = totalEnergyOut
        self._local_values["/Alarms/LowVoltage"] = maxLowVoltageAlarm
        self._local_values["/Alarms/HighVoltage"] = maxHighVoltageAlarm
        self._local_values["/Alarms/LowTemperature"] = maxLowTempAlarm
        self._local_values["/Alarms/HighTemperature"] = maxHighTempAlarm
        self._local_values["/Dc/0/Power"] = totalPower
        return True

    def publish(self):
        for k,v in self._local_values.items():
            self.service[k] = v
        return True

    def __str__(self):
        return PRODUCT_NAME


def main():
    DBusGMainLoop(set_as_default=True)
    dcSystem = DCSystemService(dbusConnection())
    GLib.timeout_add(200, dcSystem.update)
    GLib.timeout_add_seconds(1, dcSystem.publish)
    logger.info("Registered DC System Aggregator")
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()

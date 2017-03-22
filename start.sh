#!/bin/bash
cd /
cd home/pi
echo "Iniciando o watchdog"
sudo service watchdog start
echo "Iniciando o PIGPIOD"
sudo pigpiod
echo "Iniciando o script do Bluetooth"
sudo nice -20 python water_sensor_bluetooth.py > log_exec_bluetooth.txt &


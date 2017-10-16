#!/bin/bash

sudo mkdir /usr/local/bin/mongologger
sudo cp rabbitmq-mongodb.py /usr/local/bin/mongologger/rabbitmq-mongodb.py
sudo cp myconfig.json /usr/local/bin/mongologger/myconfig.json
sudo cp rabbitmq-mongodb@.service /lib/systemd/system/rabbitmq-mongodb@.service
sudo chmod 644 /lib/systemd/system/rabbitmq-mongodb@.service
sudo systemctl daemon-reload 
#sudo systemctl enable rabbitmq-mongodb@{1..10}.service

#reboot and systemctl status rabbitmq-mongodb@{1..10}.service


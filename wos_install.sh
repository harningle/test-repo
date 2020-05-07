#!/bin/bash

Font="\033[0m"
Blue="\033[36m"

echo -e "${Blue}开始宝塔面板${Font}"
wget -O install.sh http://download.bt.cn/install/install-ubuntu_6.0.sh && echo y | sudo bash install.sh

echo -e "${Blue}开始安装Python 3${Font}"
apt update
apt install python3-pip python3-setuptools python3-dev python3-wheel build-essential -y

echo -e "${Blue}开始安装MySQL${Font}"
sudo apt-get install -y mysql-server
sudo apt-get install mysql-client
sudo apt-get install -y libmysqlclient-dev

echo -e "${Blue}开始安装依赖${Font}"
mv test-repo wos
cd wos
pip3 install -r requirements.txt
rm -rf .git
rm README.md
rm requirements.txt

echo -e "${Blue}完成${Font}"
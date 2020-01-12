FROM ubuntu:latest

RUN apt-get update -y


RUN apt-get install -y build-essential checkinstall
RUN apt-get install -y libreadline-gplv2-dev  libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev

RUN apt-get install -y git
RUN apt-get install -y wget
RUN apt-get install -y vim
RUN apt-get install -y curl
RUN apt-get install -y less

RUN apt-get install -y libpq-dev

RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN apt-get install -y  python3-dev

RUN apt-get install -y unzip
RUN apt-get install -y postgresql-client

RUN pip3 install --upgrade pip

RUN pip3 install sqlalchemy
RUN pip3 install ipython
RUN pip3 install psycopg2
RUN pip3 install numpy
RUN pip3 install pandas

RUN mkdir /root/Git
RUN cd /root/Git; git pull https://github.com/jhajagos/CensusGeographyTools.git

RUN mkdir apt-get install -y wget



FROM quay.io/astronomer/astro-runtime:12.5.0
RUN apt-get -y update
RUN apt-get -y install vim nano

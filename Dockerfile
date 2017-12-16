FROM rocker/rstudio-stable:latest
COPY ./DockerConfig/requirements.R /tmp/requirements.R 
CMD ["R"]
MAINTAINER Tereza/@revolt.bi

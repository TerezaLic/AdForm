FROM rocker/r-ver:3.3.2

COPY . /home/

# Run the application
ENTRYPOINT Rscript /home/main.R /data/

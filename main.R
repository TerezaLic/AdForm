
#=======Install the libraries=========#
library(jsonlite)
library(lubridate)
library(httr)

#=======CONFIGURATION========#

library(keboola.r.docker.application)
# initialize application
app <- keboola.r.docker.application::DockerApplication$new('/data/')
app$readConfig()


# access the supplied value of 'myParameter'
apiKey<-app$getParameters()$`#apiKey`
dateFrom<-as.Date(app$getParameters()$dateFrom)
dateTo<-as.Date(app$getParameters()$dateTo)
dataProviderId<-app$getParameters()$dataProviderId
endpoint<-app$getParameters()$report

# read user input data from JSON config editor ----------TO BE UPDATED WITH new JSON form------------
# user<-app$getParameters()$user
# Password<-app$getParameters()$Password
# dataProviderId<-app$getParameters()$dataProviderId
# dateFrom<-app$getParameters()$dateFrom
# dateTo<-app$getParameters()$dateTo
# out_bucket<-app$getParameters()$bucket

##Catch config errors

if(is.null(apiKey)) stop("enter your apiKey in the user config field")

# get csv file name with full path from output mapping
outName <- app$getExpectedOutputTables()['full_path']


#=======Actual API call========#

# report Datausage je automaticky group by Agency
# report Audience vyzaduje Provider Id

url <- "https://api.adform.com/"
req <- httr::GET(url,path=endpoint,query=list(dataProviderId=dataProviderId,from=dateFrom,to=dateTo) ,httr::add_headers(Authorization = apiKey))
json <- httr::content(req, as = "text")
Datausage<-fromJSON(json)


# write output data
write.csv(Datausage,"tests/data/out/tables/result.csv",row.names = FALSE)

# write table metadata






#=======Install the libraries=========#
library(lubridate)
library(httr)
library(jsonlite)
library(keboola.r.docker.application)

#=======CONFIGURATION========#

# initialize application
app <- DockerApplication$new('/data/')
app$readConfig()


# access the supplied value of 'myParameter'
dateFrom<-app$getParameters()$dateFrom
dateTo<-app$getParameters()$dateTo
grouping<-app$getParameters()$groupBy
ContentType<-"text/csv"
user<-app$getParameters()$user
password<-app$getParameters()$`#pass`
dataProviderId<-"11392"


#=======Actual API call========#

# get session token

url <- "https://api.adform.com/"
req <- httr::POST(url,path="v1/dmp/token",body=list(grant_type="password",username=user,password=password),encode = "form")
json <- httr::content(req, as = "text")
returnData<-fromJSON(json)
apiKey <- paste("Bearer ",returnData$access_token)

# catch login errors
if (req$status_code != 200) stop("unauthorized - verify username & password")

# get list of data providers
endpoint0<-"/v1/dmp/dataproviders"
req0 <-httr::GET(url,path=endpoint0,httr::add_headers(Accept = ContentType,Authorization = apiKey))
dataProviders<-httr::content(req0, as="parse")
dataProvidersId<-list(dataProviders$Id)




# create IN tables

endpoint1<-"v2/dmp/reports/datausage"
req1 <- httr::GET(url,path=endpoint1,query=list(groupBy=grouping,dataProviderId=dataProvidersId$Id ,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
datausage<-httr::content(req1, as="parse")
write.csv(datausage,"out/tables/datausage.csv",row.names = FALSE)

remove(req1,endpoint1,datausage)

endpoint2<-"v2/dmp/reports/audience"
req2 <- httr::GET(url,path=endpoint2,query=list(groupBy=grouping,dataProviderId=dataProviderId,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
audience<-httr::content(req2, as="parse")
write.csv(audience,"out/tables/audience.csv",row.names = FALSE)

remove(req2,endpoint2,audience)

endpoint3<-"/v2/dmp/dataproviders/{dataProviderId}/audience"
req3 <- httr::GET(url,path=endpoint3,query=list(dataProviderId=dataProviderId,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
DataProvidersAudience<-httr::content(req3, as="parse")
write.csv(DataProvidersAudience,"out/tables/DataProvidersAudience.csv",row.names = FALSE)

remove(req3,endpoint3,DataProvidersAudience)

endpoint4<-"v2/dmp/reports/billing/overall"
req4 <- httr::GET(url,path=endpoint4,query=list(dataProviderId=dataProviderId,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
billing<-httr::content(req4, as="parse")
write.csv(billing,"out/tables/billing.csv",row.names = FALSE)

remove(req4,endpoint4,billing)


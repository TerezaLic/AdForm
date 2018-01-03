

#=======Install the libraries=========#
library(lubridate)
library(httr)
library('keboola.r.docker.application')

#=======CONFIGURATION========#

# initialize application
app <- keboola.r.docker.application::DockerApplication$new('/data/')
app$readConfig()


# access the supplied value of 'myParameter'
apiKey<-app$getParameters()$`#apiKey`
dateFrom<-app$getParameters()$dateFrom
dateTo<-app$getParameters()$dateTo
dataProviderId<-app$getParameters()$dataProviderId
grouping<-app$getParameters()$groupBy
ContentType<-"text/csv"

# read user input data from JSON config editor ----------TO BE UPDATED WITH new JSON form------------
# user<-app$getParameters()$user
# Password<-app$getParameters()$Password
# out_bucket<-app$getParameters()$bucket

##Catch config errors

if(is.null(apiKey)) stop("enter your apiKey in the user config field")


#=======Actual API call========#

# create IN tables

url <- "https://api.adform.com/"


endpoint1<-"v2/dmp/reports/datausage"
req1 <- httr::GET(url,path=endpoint1,query=list(groupBy=grouping,dataProviderId=dataProviderId,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
datausage<-httr::content(req1, as="parse")
write.csv(datausage,"tests/data/in/tables/datausage.csv",row.names = FALSE)

endpoint2<-"v2/dmp/reports/audience"
req2 <- httr::GET(url,path=endpoint2,query=list(groupBy=grouping,dataProviderId=dataProviderId,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
audience<-httr::content(req2, as="parse")
write.csv(audience,"tests/data/in/tables/audience.csv",row.names = FALSE)

endpoint3<-"/v2/dmp/dataproviders/{dataProviderId}/audience"
req3 <- httr::GET(url,path=endpoint3,query=list(dataProviderId=dataProviderId,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
DataProvidersAudience<-httr::content(req3, as="parse")
write.csv(DataProvidersAudience,"tests/data/in/tables/DataProvidersAudience.csv",row.names = FALSE)

endpoint4<-"v2/dmp/reports/billing/overall"
req4 <- httr::GET(url,path=endpoint4,query=list(dataProviderId=dataProviderId,from=dateFrom,to=dateTo) ,httr::add_headers(Accept = ContentType,Authorization = apiKey))
billing<-httr::content(req4, as="parse")
write.csv(billing,"tests/data/in/tables/billing.csv",row.names = FALSE)

remove(req1,req2,req3, req4)
remove(ContentType,endpoint1,endpoint2,endpoint3,endpoint4)

# transformation
datausageTOTAL<-data.frame(dateFrom,dateTo,billing$DataProvider,sum(datausage$Revenue),sum(datausage$RevenueInAdvertiserCurrency),sum(datausage$`Adform Gross Revenue`),sum(datausage$DataProviderRevenue), sum(datausage$DataProviderRevenueInAdvertiserCurrency),sum(datausage$`Owner Net Revenue (EUR)`),sum(datausage$AdformRevenue),sum(datausage$`Adform Net Revenue(EUR)`),sum(datausage$AdformRevenueInAdvertiserCurrency))

# create OUT tables
write.csv(billing,"tests/data/out/tables/billing.csv",row.names = FALSE)
write.csv(datausageTOTAL,"tests/data/out/tables/datausage.csv",row.names = FALSE)
write.csv(audience,"tests/data/out/tables/audience.csv",row.names = FALSE)
write.csv(DataProvidersAudience,"tests/data/in/tables/DataProvidersAudience.csv",row.names = FALSE)

Adform DMP reports API

Extractor for Keboola allowing automated downloads from AdForm Data Management Platform.

Application input:

- AdForm DMP user account & password
- Period specification
- Report paramters (group by)

Please contact Adform support if any issue with login credentials.

This component allows you to extract:

- Usage (Revenue and Impressions) Reports with provider Id, group by parameters
- Audience reports with provider Id, group by parameters
- Overall audience stats by data provider with provider Id by parameter
- Billing overall report without any paramater User can define "group by" parametr, provider Ids are used from AdForm input.

Configuration does not allow to specify the primary keys of imported tables. Instead, user must set the primary keys manually in the KBC UI within the STORAGE section after the first successfull import.

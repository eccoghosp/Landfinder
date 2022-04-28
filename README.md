# Landfinder
This is a web scraper that will get all the properties from landwatch.com for one states or all states. It deposits the results into a SQL server.

Currently, it is set up only to save to a local MSSQL Server using windows authentification. There are timers that wait to help reduce the load on landwatch's server, but I also recommend using a vpn to run this. I will add dynamic headers and options to cycle through IP addresses in the future. 

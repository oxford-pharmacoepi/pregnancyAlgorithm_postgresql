# Load libraries
library("DatabaseConnector")
library("here")

# Set connection details
server     <-Sys.getenv("DB_SERVER_gold_202207")
user       <-Sys.getenv("DB_USER")
password   <-Sys.getenv("DB_PASSWORD")
port       <-Sys.getenv("DB_PORT")
host       <-Sys.getenv("DB_HOST")



# connectionDetails <-DatabaseConnector::downloadJdbcDrivers("postgresql",here::here())


# Connect to the database via database connector to run the algorithm
connectionDetails <-DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                               server = server,
                                                               user = user,
                                                               password = password,
                                                               port = port,
                                                               pathToDriver = here::here())


targetDialect <-"postgresql"
cdmDatabaseSchema <-"public"
vocabularyDatabaseSchema <-"public"
resultsDatabaseSchema <-"results"

connection = connect(connectionDetails)
#disconnect(connection)

source("~/pregnancyAlgorithm_postgresql/R/main.R")
init (connectionDetails, resultsDatabaseSchema, useMppBulkLoad = FALSE)
execute (connectionDetails, cdmDatabaseSchema, resultsDatabaseSchema)


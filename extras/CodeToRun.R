# Load libraries
library("DatabaseConnector")
library("here")

# Set connection details
server     <-Sys.getenv("DB_SERVER_gold_202207")
user       <-Sys.getenv("DB_USER")
password   <-Sys.getenv("DB_PASSWORD")
port       <-Sys.getenv("DB_PORT")
host       <-Sys.getenv("DB_HOST")

# Connect to the database

#connectionDetails <-DatabaseConnector::downloadJdbcDrivers("postgresql",here::here())

connectionDetails <-DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                               server = server,
                                                               user = user,
                                                               password = password,
                                                               port = port,
                                                               pathToDriver = here::here())


targetDialect <-"postgresql"
cdmDatabaseSchema <-"public_100k"
vocabularyDatabaseSchema <-"public_100k"
resultsDatabaseSchema <-"results"

connection = connect(connectionDetails)
#disconnect(connection)

source("~/PregnancyAlgorithm/R/main.R")
init (connectionDetails, resultsDatabaseSchema, useMppBulkLoad = FALSE)
execute (connectionDetails, cdmDatabaseSchema, resultsDatabaseSchema)

## connect to a cdm instance
server_dbi <- Sys.getenv("DB_SERVER_DBI_gold_202207")
user       <- Sys.getenv("DB_USER")
password   <- Sys.getenv("DB_PASSWORD")
port       <- Sys.getenv("DB_PORT")
host       <- Sys.getenv("DB_HOST")

db <- dbConnect(RPostgres::Postgres(),
                dbname = server_dbi,
                port = port,
                host = host,
                user = user,
                password = password
)

cdm <- cdm_from_con(
  db,
  cdm_schema = "public_100k",
  write_schema = "results"
)

cdm$motherTable <- tbl(db, in_schema("results","pregnancy_episodes"))

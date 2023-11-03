## change the motherTable to the correct column names and concept ids


## connect to a cdm instance to do the postprocessing
server_dbi <- Sys.getenv("DB_SERVER_DBI_gold_202207")
user       <- Sys.getenv("DB_USER")
password   <- Sys.getenv("DB_PASSWORD")
port       <- Sys.getenv("DB_PORT")
host       <- Sys.getenv("DB_HOST")

library(CDMConnector)
library(dplyr)

db <- dbConnect(RPostgres::Postgres(),
                dbname = server_dbi,
                port = port,
                host = host,
                user = user,
                password = password
)

cdm <- cdm_from_con(
  db,
  cdm_schema = "public",
  write_schema = "results"
)

cdm$motherTable <- tbl(db, in_schema("results","pregnancy_episodes"))


# motherTable <- cdm$motherTable %>% collect()
# unique(motherTable$original_outcome) 
## "SA"    "DELIV" "AB"    "LB"    "ECT"   "SB"   

# explor <- motherTable %>% select("original_outcome","outcome") %>% distinct(original_outcome,outcome)


motherTable <- cdm$motherTable %>% 
  dplyr::transmute(person_id = person_id,
                pregnancy_id = row_number(),
                pregnancy_start_date = as.Date(episode_start_date),
                pregnancy_end_date = as.Date(episode_end_date),
                gestational_length_in_day = episode_length,
                original_outcome = as.numeric(ifelse(
                  original_outcome=="SA",4067106,ifelse(
                    original_outcome=="DELIV",4092289,ifelse(
                      original_outcome %in% c("AB","ECT"),4081422,ifelse(
                        original_outcome=="LB",4092289,ifelse(
                          original_outcome=="SB",4067106,0)))))),
                pregnancy_mode_delivery = NA,
                pregnancy_single = NA,
                prev_pregnancy_gravidity = episode - 1) %>% collect()

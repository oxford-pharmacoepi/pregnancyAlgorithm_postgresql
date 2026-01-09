## change the motherTable to the correct column names and concept ids


## connect to a cdm instance to do the postprocessing
server_dbi <- "cdm_gold_202507"
user       <- Sys.getenv("DB_USER")
password   <- Sys.getenv("DB_PASSWORD")
port       <- Sys.getenv("DB_PORT")
host       <- Sys.getenv("DB_HOST")

library(CDMConnector)
library(dplyr)
library(DBI)
library(dplyr)


db <- dbConnect(RPostgres::Postgres(),
                dbname = server_dbi,
                port = port,
                host = host,
                user = user,
                password = password
)

cdm <- cdmFromCon(
  db,
  cdmSchema = "public",
  writeSchema = "results"
)

cdm$pregnancy_episodes <- tbl(
  db, inSchema(schema = "results", table = "pregnancy_episode")
) |>
  compute(
    name = inSchema("results", "pregnancy_episodes"),
    temporary = FALSE,
    overwrite = TRUE
  )

cdm$pregnancy_episodes <- cdm$pregnancy_episodes %>% 
  dplyr::mutate(
    pregnancy_id = as.integer(row_number()),
    person_id = as.integer(person_id),
    pregnancy_start_date = as.Date(.data$episode_start_date),
    pregnancy_end_date = as.Date(.data$episode_end_date),
    gestational_length_in_day = as.integer(.data$episode_length),
    pregnancy_outcome = dplyr::case_when(
      .data$original_outcome == "SA" ~ 4067106L, # spontaneous abortion --> Miscarriage
      .data$original_outcome == "DELIV" ~ 4092289L, # delivery --> Livebirth
      .data$original_outcome %in% c("AB","ECT") ~ 4081422L, # abortion/elective termination --> 	Elective termination of pregnancy
      .data$original_outcome == "LB" ~ 4092289L, # livebirth --> Livebirth
      .data$original_outcome == "SB" ~ 443213L, # stillbirth --> Stillbirth
      .default = 0L
    ),
    # Stillbirth if >20 weeks of pregnancy:
    pregnancy_outcome = case_when(
      pregnancy_outcome == 4067106 & gestational_length_in_day >= 20*7 ~ 443213L, # miscarriage to stillbirth
      pregnancy_outcome == 443213 & gestational_length_in_day < 20*7 ~ 4067106L, # stillbirth to miscarriage
      .default = .data$pregnancy_outcome
    ),
    pregnancy_mode_delivery = 0L,
    pregnancy_single = 0L,
    pregnancy_marital_status = 0L,
    pregnancy_number_fetuses = NA_integer_,
    pregnancy_number_liveborn = NA_integer_,
    prev_pregnancy_parity = NA_integer_,
    prev_pregnancy_gravidity = as.integer(episode - 1),
    prev_livebirth_number = NA_integer_,
    prev_still_misc_number = NA_integer_,
    prev_top_number = NA_integer_,
    pre_pregnancy_bmi = NA_integer_,
    pregnancy_folic = 0L,
    pregnancy_art = 0L, 
    pregnancy_outcome_source_value = varchar(50),
    pregnancy_mode_delivery_source_value = varchar(50)
  ) |>
  dplyr::select(dplyr::all_of(c(
    "pregnancy_id", "person_id", "pregnancy_start_date", "pregnancy_end_date",
    "gestational_length_in_day", "pregnancy_outcome", "pregnancy_mode_delivery",
    "pregnancy_single", "pregnancy_marital_status", "pregnancy_number_fetuses",
    "pregnancy_number_liveborn", "prev_pregnancy_parity", "prev_pregnancy_gravidity",
    "prev_livebirth_number", "prev_still_misc_number", "prev_top_number", 
    "pre_pregnancy_bmi", "pregnancy_folic", "pregnancy_art", 
    "pregnancy_outcome_source_value", "pregnancy_mode_delivery_source_value"
  ))) |>
  dplyr::compute(name = "pregnancy_episodes", temporary = FALSE, overwrite = TRUE)
  
  
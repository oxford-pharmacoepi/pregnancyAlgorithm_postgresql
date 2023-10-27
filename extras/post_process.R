## change the motherTable to the correct column names and concept ids


# unique(motherTable$original_outcome) : "SA"    "DELIV" "AB"    "LB"    "ECT"   "SB"   

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

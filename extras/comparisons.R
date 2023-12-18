
## comparisons post postprocessing

## gestational length matcho paper

motherTable_matcho <- motherTable %>%
  mutate(
    pregnancy_year = lubridate::year(pregnancy_start_date)
  ) %>%
  filter(pregnancy_year %in% 1987:2013,
         original_outcome %in% c("LB","DELIV")
  )


# figure 4 data  -----
figure_4_data <- motherTable_matcho %>%
  filter(gestational_length_in_day<310) %>%
  filter(gestational_length_in_day!=0) %>%
  select(gestational_length_in_day) 

weeks <- seq(0,310, 10)
figure_4_data$week <- NA

for(i in 1:(length(weeks)-1)){
  start <- weeks[[i]]
  end <- weeks[[i+1]]
  name<-i-1
  
  figure_4_data <- figure_4_data %>%
    mutate(week=ifelse(gestational_length_in_day >= start &
                         gestational_length_in_day < end  ,
                       name, week))
}

figure_4_data <- figure_4_data %>%
  group_by(week) %>%
  count() 

write.csv(figure_4_data,"figure_4_data.csv")

## comparison "CPRD Pregnancy register paper" minassian 2019 PDS
# n pregnancies per woman

mothertable_minassian <- motherTable %>%
  select(person_id,pregnancy_id) %>%
  group_by(person_id) %>%
  summarise(n_pregnancies = n()) %>%
  count(n_pregnancies)


# n pregnancy outcomes

mothertable_minassian <- motherTable %>%
  select(original_outcome,pregnancy_id) %>%
  group_by(original_outcome) %>%
  count(original_outcome)

write.csv(mothertable_minassian,"mothertable_minassian.csv")

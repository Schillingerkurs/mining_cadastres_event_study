


list.of.packages <- c("tidyverse", "fixest","here","glue")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)


library(tidyverse);library(fixest); library(here); library(glue)


library(ggiplot)


load(here("data","interim","country_panel_mining_aid.RData"))


# Import as panel , label "df", remove long file name
df = panel(country_panel_mining_aid, ~iso3+year)

rm(country_panel_mining_aid,list.of.packages, new.packages)
gc()


colnames(df)





model_high_corruption  = feols(tot_res_rev_dummy ~ mining_aid_spend + sunab(first_year_aid_treatment, year) | iso3 + year, df %>% filter(pol_cor_quantile == "high") )
model_low_corruption  = feols(tot_res_rev_dummy ~ mining_aid_spend + sunab(first_year_aid_treatment, year) | iso3 + year, df %>% filter(pol_cor_quantile == "middle_high"))

ggiplot(list( "High corruption" = model_high_corruption ,
              "Low corruption" = model_low_corruption),
        
        main = "Dummy resource revenue")





  coeftable_high_corruption <- data.frame(model_high_corruption$coeftable)
       
       
       
coeftable_low_corruption <- data.frame(resourcetaxes_low_corruption$coeftable)


write.csv(coeftable_high_corruption, file=
            here("data","interim","coeftables","coeftable_high_corruption.csv"))
            
         
write.csv(coeftable_low_corruption, file=
            here("data","interim","coeftables","coeftable_low_corruption.csv"))
## Read in and parse needed NHANES data 
## by: David A Hughes
## date: Nov 4th 2025

## ---- load libraries ----
library(tidyverse)
library(foreign)
library(cli)

## ---- read in the data ----

### ---- read in function ----
read_dir = function(data_dir, vars_to_select, new_var_names ){
  # data_dir = "../data/nhanes_data/Demographics/Demographic Variables _ Sample Weights/"
  years = list.files(data_dir)
  
  out = map(years, function(x) {
    cat("Running iteration", x, "\n")
    files = list.files( paste0(data_dir, x, "/") )
    files = files[grep(".xpt", files)]
    f = paste0(data_dir, x, "/", files)
    d = tibble( foreign::read.xport(f) )
    
    d = d |> select( vars_to_select ) 
    
    ## rename variables to be consistent
    colnames(d) = new_var_names 
    
    ## Add sampling year
    d = d |> mutate(sampling_year = x, .after = id)
    d = tibble(d)
    return( d )
  })
  
  names(out) = years
  return(out)
}


cli::cli_h1("NHANES data assembly")
### ---- Demographics ----
cli::cli_inform(c("i" = "Loading Demographics data"))

demo1 = read_dir(data_dir = "../data/raw/nhanes_data/Demographics/Demographic Variables _ Sample Weights/",
                vars_to_select = c("SEQN", "RIAGENDR", "RIDAGEYR", "RIDRETH1"),
                new_var_names = c("id", "sex", "age", "ethnicity") )
demo2 = read_dir(data_dir = "../data/raw/nhanes_data/Demographics/Demographic Variables and Sample Weights/",
                 vars_to_select = c("SEQN", "RIAGENDR", "RIDAGEYR", "RIDRETH1"),
                 new_var_names = c("id", "sex", "age", "ethnicity") )
demo = c(demo1, demo2)

cli::cli_inform(c("v" = "Loaded {length(demo)} demographic cycles"))

### ---- Examination Data ----
#### ---- DXA ----
cli_inform(c("i" = "Loading DXA data"))

dxa = read_dir(data_dir = "../data/raw/nhanes_data/Examination/Dual-Energy X-ray Absorptiometry - Whole Body/",
               vars_to_select = c("SEQN", "DXAEXSTS", "DXDTOFAT", "DXDTOBMC", "DXDTOLE", "DXDTOPF"),
               new_var_names = c("id", "dxa_comment", "total_fat", "total_bone", "total_lean", "percent_fat") )

cli::cli_inform(c("v" = "Loaded {length(dxa)} DXA cycles"))

#### ---- Body Measures ----
cli_inform(c("i" = "Loading Body Measures data"))

body = read_dir(data_dir = "../data/raw/nhanes_data/Examination/Body Measures/",
               vars_to_select = c("SEQN", "BMXWT", "BMIWT", "BMXHT", "BMIHT", "BMXWAIST"),
               new_var_names = c("id", "weight", "weight_comment", "height", "height_comment", "waist_circumference") )

## remove the 2017-2018 cycle as it is redundant becaus there is a 2017-2020 cycle
body = body[ names(body) != "2017-2018" ]

cli::cli_inform(c("v" = "Loaded {length(body)} Body Measures cycles"))

cli::cli_alert_success("All raw data loaded successfully")

## ---- data filtering / processing ----
cli::cli_h2("Data filtering and processing")

### ---- DXA filtering ----
for(i in 1:length(dxa)){
  dxa[[i]] = dxa[[i]] |> filter( dxa_comment == 1 ) |> na.omit()
}

## for each individual average across the replicate measurements
for(i in 1:length(dxa)){
  dxa[[i]] = dxa[[i]] |> 
    group_by(id) |> 
    summarise(
      total_fat = mean(total_fat, na.rm = T),
      total_bone = mean(total_bone, na.rm = T),
      total_lean = mean(total_lean, na.rm = T),
      percent_fat = mean(percent_fat, na.rm = T)
    )  |> ungroup()
}


## ---- merge data ----
cli_inform(c("i" = "Merging all datasets"))

### ---- rbind lists ----
demo_temp = bind_rows(demo)
body_temp = bind_rows(body)
dxa_temp = bind_rows(dxa)

### ---- edit / QC height & weight data ----
## weight comments
w = which( !is.na(body_temp$weight_comment) )
if(length(w) > 0){
  body_temp = body_temp[-w,]
}
## height comments
w = which( !is.na(body_temp$height_comment) )
if(length(w) > 0){
  body_temp = body_temp[-w,]
}

body_temp = body_temp |> select( -c(weight_comment, height_comment) ) |> na.omit()


### ---- rbind data ----
all_data =  demo_temp |>
  left_join( body_temp, by = c("id", "sampling_year") ) |>
  left_join( dxa_temp, by = "id" )

### ---- remove duplicates ----
all_data = all_data |> distinct()
# dim(all_data)

### ---- remove samples with no weight or height data ----
all_data = all_data |> filter( !is.na(weight) ) |> filter( !is.na(height) )
# dim(all_data)


## ---- save data ----
cli::cli_h2("Saving processed data")

write_csv(all_data, "../data/processed/nhanes_combined_data.csv")
saveRDS(all_data, "../data/processed/nhanes_combined_data.rds")


cli::cli_alert_success("Processed data saved successfully")

cli::cli_h1("Data wrangling complete")


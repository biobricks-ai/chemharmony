pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

# define folder
Sys.setenv(BBLIB="/mnt/biobricks/")



tox21  <- biobricks::brick_load("tox21")
invisible(safely(fs::dir_delete)("cache/tox21"))
outputdir <- fs::dir_create("cache/tox21",recurse=T)
writeds  <- function(df,name){ arrow::write_dataset(df,fs::path(outputdir,name)) }

activity <- chembl$activities.parquet
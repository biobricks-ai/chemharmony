pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

invisible(safely(fs::dir_delete)("cache/harmonized"))
outputdir <- fs::dir_create("cache/harmonized", recurse = TRUE)
writeds <- function(df, name) {
    arrow::write_dataset(df, fs::path(outputdir, name))
  }

toxvaldb <- open_dataset("./cache/toxvaldb/substances.parquet") |>
    collect()
chembl <- open_dataset("./cache/chembl/substances.parquet") |>
    collect()
tox21 <- open_dataset("./cache/tox21/substances.parquet") |>
    collect()

harmonized <- rbind(toxvaldb, chembl, tox21)

writeds(harmonized, "substances.parquet")

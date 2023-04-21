pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, glue, future)
reticulate::use_virtualenv("./env", required = TRUE)

# ACTIVITIES =============================================================
# create a brick/activities.parquet and add smiles
afiles <- fs::dir_ls("staging", recurse=T, glob="*.parquet", type="file")
afiles <- afiles |> purrr::keep(\(x){ grepl("activities.parquet",x) })

# create multicore future plan with length(afiles) workers
process_batch <- function(batch){
  pacman::p_load(biobricks, tidyverse, arrow, uuid)
  reticulate::use_virtualenv("./env", required = TRUE)
  
  rdkit <- reticulate::import("rdkit")  
  inchi2smi <- purrr::possibly(function(inch){
    mol <- rdkit$Chem$MolFromInchi(inch,sanitize=TRUE)
    rdkit$Chem$MolToSmiles(mol)
  }, otherwise = NA_character_, quiet = TRUE)

  batch$smiles = purrr::map_chr(batch$inchi, inchi2smi) 
  batch |> dplyr::select(source, aid, sid, pid, inchi, smiles, value, binary_value)
}


td <- fs::dir_create(tempdir(),"stage1")
i <- 1
for(file in afiles){
  cat("doing file",i,"out of",length(afiles),"\n")
  sourcename <- strsplit(file,"/")[[1]][[2]]
  df <- arrow::open_dataset(file)  
  df <- df |> mutate(source = sourcename)
  df <- df |> mutate(value = tolower(value)) |>
    mutate(value = case_when(
      value == "active" ~ "positive",
      value == "inactive" ~ "negative",
      value == "active agonist" ~ "positive",
      value == "active antagonist" ~ "negative",
      value == "increases" ~ "positive",
      value == "decreases" ~ "negative",
      value == "positive" ~ "positive",
      value == "negative" ~ "negative"
    )) |> mutate(binary_value = case_when(
      value == "positive" ~ 1,
      value == "negative" ~ 0
    ))
  
  path <- fs::dir_create(td, uuid::UUIDgenerate())
  arrow::write_dataset(df, max_rows_per_file=2e6, path=path)
  i <- i + 1
}

a2files <- fs::dir_ls(td, recurse=T, glob="*.parquet", type="file")
i <- 1
td2 <- fs::dir_create("staging/activities.parquet")
workers <- 45
future::plan(future::multisession, workers=workers)
for(file in a2files){
  cat("doing file",i,"out of",length(a2files),"\n")
  df <- arrow::read_parquet(file, as_dataframe=TRUE)
  
  # create numworkers batches and process in parallel
  batches <- rep(1:workers, length.out = nrow(df))
  batches <- split(df, batches)

  # process each batch in parallel and wait for all to finish
  cat("starting batches\n")
  df <- furrr::future_map_dfr(batches, ~ process_batch(.))
  path <- fs::path(td2,uuid::UUIDgenerate(),ext="parquet")
  arrow::write_parquet(df,path)
  i <- i + 1
}

act <- arrow::open_dataset("staging/activities.parquet") 

# TODO need to do something better here
# set pid+inchi value to positive if it is positive in any duplicate 
act <- act |> filter(!is.na(inchi)) |> filter(!is.na(smiles))
act <- act |> group_by(pid,inchi) |> summarise(binvalue=max(binary_value)) |> ungroup()
act <- act |> mutate(value = ifelse(binvalue==1,"positive","negative"))
act |> arrow::write_dataset("brick/activities.parquet", max_rows_per_file = 1e6)

# SUBSTANCES =============================================================
sfiles <- fs::dir_ls("staging", recurse=T, glob="*.parquet", type="file")
sfiles <- sfiles |> purrr::keep(\(x){ grepl("substances.parquet",x) })

outdir <- fs::dir_create("staging/substances.parquet")
purrr::walk(sfiles, \(file){
  a <- arrow::open_dataset(file) |> collect()
  a <- a |> mutate(source = strsplit(file,"/")[[1]][[2]])
  a |> arrow::write_parquet(fs::path(outdir,uuid::UUIDgenerate(),ext="parquet"))
})

substances <- arrow::open_dataset("staging/substances.parquet")
substances |> arrow::write_dataset("brick/substances.parquet", max_rows_per_file = 2e6)

# PROPERTIES =============================================================
pfiles <- fs::dir_ls("staging", recurse=T, glob="*.parquet", type="file")
pfiles <- pfiles |> purrr::keep(\(x){ grepl("properties.parquet",x) })

outdir <- fs::dir_create("staging/properties.parquet")
purrr::walk(pfiles, \(file){
  a <- arrow::open_dataset(file) |> collect()
  a <- a |> mutate(source = strsplit(file,"/")[[1]][[2]])
  a |> arrow::write_parquet(fs::path(outdir,uuid::UUIDgenerate(),ext="parquet"))
})

properties <- arrow::open_dataset("staging/properties.parquet")
properties |> arrow::write_dataset("brick/properties.parquet", max_rows_per_file = 2e6)

# CLEAN UP ===============================================================
fs::dir_delete("staging/substances.parquet")
fs::dir_delete("staging/properties.parquet")
fs::dir_delete("staging/activities.parquet")

# LOOK FOR DUPLICAT PID INCHI AND HIGHLIGHT BAD SOURCES
# df <- arrow::open_dataset("brick/activities.parquet") |> collect()
# bad_examples <- df |> count(source,pid,inchi) |> filter(n>1)
# bad_examples |> count(source) |> arrange(desc(n)) 

pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, glue, future, futile.logger)
reticulate::use_virtualenv("./env", required = TRUE)

# Setup logger
flog.appender(appender.file("log/harmonize.log"))
flog.threshold(INFO) # Set the log level (DEBUG, INFO, WARN, ERROR)

workers <- 30
future::plan(future::multisession, workers=workers)

flog.info("Starting the script...")

# CLEAN UP PREVIOUS RUNS =================================================
del_if_exists <- \(x){ if(fs::dir_exists(x)) fs::dir_delete(x); flog.info(sprintf("deleting %s",x)) }
del_if_exists("staging/tmp")

# ACTIVITIES =============================================================
afiles <- fs::dir_ls("staging", recurse=T, glob="*.parquet", type="file")
afiles <- afiles |> purrr::keep(\(x){ grepl("activities.parquet",x) })

staging_dir <- fs::path("staging","tmp","activities.parquet") |> fs::dir_create()
flog.info("created staging/tmp/activities.parquet")

process_inputs <- function(file, index, writepath){
  pacman::p_load(biobricks, tidyverse, arrow, uuid)
  reticulate::use_virtualenv("./env", required = TRUE)

  # add sourcename, binary values and smiles to activities
  sourcename <- strsplit(file,"/")[[1]][[2]]
  df <- arrow::open_dataset(file) |> collect()
  df <- df |> mutate(source = sourcename)
  df <- df |> mutate(binary_value = ifelse(value == "positive", 1, 0))
  
  rdkit <- reticulate::import("rdkit")  
  inchi2smi <- purrr::possibly(function(inch){
    mol <- rdkit$Chem$MolFromInchi(inch,sanitize=TRUE)
    rdkit$Chem$MolToSmiles(mol)
  }, otherwise = NA_character_, quiet = TRUE)

  inchi2smi <- df |> select(inchi) |> distinct() |> mutate(smiles = purrr::map_chr(inchi, inchi2smi)) 
  df <- df |> inner_join(inchi2smi,by="inchi") |> 
    dplyr::select(source, aid, sid, pid, inchi, smiles, value, binary_value)


  arrow::write_dataset(df, max_rows_per_file=2e6, path = writepath,
    existing_data_behavior = "overwrite", 
    basename_template = sprintf("%s-%s-{i}.parquet",sourcename,index))
}

flog.info("processing staged activities")
results <- purrr::map(seq_along(afiles), \(i){ 
  cat(sprintf("doing file %s-%s\n",afiles[i],i))
  future::future(process_inputs(afiles[i], i, staging_dir)) 
})
purrr::walk(results, function(fut){
  tryCatch({ future::value(fut) }, error = function(e) {
    flog.error("Error in processing future: %s", e$message)
    stop("Error in processing staged activities")
  })
})
flog.info("finished processing staged activities")

# filter to only keep substances with >50 positive and >50 negative activities
act <- arrow::open_dataset(staging_dir) |> filter(!is.na(inchi), !is.na(smiles))
pids <- act |> count(pid, binary_value) |> filter(n > 50) |> count(pid) |> filter(n==2) |> pull(pid, as_vector=TRUE)
act |> filter(pid %in% pids) |> arrow::write_dataset("brick/activities.parquet", max_rows_per_file = 2e6)
flog.info("wrote brick/activities.parquet")

arrow::open_dataset("brick/activities.parquet") |> nrow() # 15 352 907

# SUBSTANCES =============================================================
flog.info("building brick/substances.parquet")
sfiles <- fs::dir_ls("staging", recurse=T, glob="*.parquet", type="file")
sfiles <- sfiles |> purrr::keep(\(x){ grepl("substances.parquet",x) })

outdir <- fs::path("staging/tmp/substances.parquet") |> fs::dir_create()
purrr::walk(sfiles, \(file){
  a <- arrow::open_dataset(file) |> collect()
  a <- a |> mutate(source = strsplit(file,"/")[[1]][[2]])
  a |> arrow::write_parquet(fs::path(outdir,uuid::UUIDgenerate(),ext="parquet"))
}, .progress = TRUE)

substances <- arrow::open_dataset(outdir)
substances |> arrow::write_dataset("brick/substances.parquet", max_rows_per_file = 2e6)
arrow::open_dataset("brick/substances.parquet") |> nrow() # 118 373 043
flog.info("built brick/substances.parquet")

# PROPERTIES =============================================================
flog.info("building brick/properties.parquet")
pfiles <- fs::dir_ls("staging", recurse=T, glob="*.parquet", type="file")
pfiles <- pfiles |> purrr::keep(\(x){ grepl("properties.parquet",x) })

outdir <- fs::path("staging/tmp/properties") |> fs::dir_create()
purrr::walk(pfiles,function(pfile){
  a <- arrow::read_parquet(pfile) |> collect()
  a <- a |> mutate(source = strsplit(pfile,"/")[[1]][[2]])
  a |> arrow::write_parquet(fs::path(outdir,uuid::UUIDgenerate(),ext="parquet"))
}, .progress = TRUE)

properties <- arrow::open_dataset(outdir)
properties |> nrow() # 248 985 893

properties <- properties |> filter(pid %in% pids)
properties |> arrow::write_dataset("brick/properties.parquet", max_rows_per_file = 1e6)
arrow::open_dataset("brick/properties.parquet") |> nrow() # 7 920
flog.info("built brick/properties.parquet")
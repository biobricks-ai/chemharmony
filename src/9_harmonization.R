pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, kit, glue)
reticulate::use_virtualenv("./env", required = TRUE)
pacman::p_load(biobricks, tidyverse, memoise)

rdkit <- reticulate::import("rdkit")
inchi2smi <- purrr::possibly(function(smi){
  mol <- rdkit$Chem$MolFromInchi(smi)
  rdkit$Chem$MolToSmiles(mol)
}, otherwise = NA_character_, quiet = TRUE)
inchi2smi <- memoise::memoise(inchi2smi)

# ACTIVITIES =============================================================
# create a brick/activities.parquet and add smiles
parquets <- fs::dir_ls("staging", recurse=T, glob="*/activities.parquet")
outdir <- fs::dir_create("staging/activities.parquet")
purrr::walk(parquets, \(file){
  a <- arrow::open_dataset(file) |> collect()
  a <- a |> mutate(smiles = purrr::map_chr(inchi, inchi2smi, .progress = T))
  a |> select(aid, sid, pid, inchi, smiles, value)
  a |> arrow::write_parquet(fs::path(outdir,uuid::UUIDgenerate(),ext="parquet"))
})

activities <- arrow::open_dataset("staging/activities.parquet") |> collect() |> distinct()

# SUBSTANCES =============================================================
parquets <- fs::dir_ls("staging", recurse=T, glob="*/substances.parquet")
outdir <- fs::dir_create("staging/substances.parquet")
purrr::walk(parquets, \(file){
  a <- arrow::open_dataset(file) |> collect()
  a |> arrow::write_parquet(fs::path(outdir,uuid::UUIDgenerate(),ext="parquet"))
})

substances <- arrow::open_dataset("staging/substances.parquet")
substances |> arrow::write_dataset("brick/substances.parquet", max_rows_per_file = 1e6)

# PROPERTIES =============================================================
parquets <- fs::dir_ls("staging", recurse=T, glob="*/properties.parquet")
outdir <- fs::dir_create("staging/properties.parquet")
purrr::walk(parquets, \(file){
  a <- arrow::open_dataset(file) |> collect()
  a |> arrow::write_parquet(fs::path(outdir,uuid::UUIDgenerate(),ext="parquet"))
})

properties <- arrow::open_dataset("staging/properties.parquet")
properties |> arrow::write_dataset("brick/properties.parquet", max_rows_per_file = 1e6)

# CLEAN UP ===============================================================
fs::dir_delete("staging/substances.parquet")
fs::dir_delete("staging/properties.parquet")
fs::dir_delete("staging/activities.parquet")
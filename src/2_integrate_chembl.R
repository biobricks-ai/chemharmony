pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

biobricks::brick_install("chembl")
biobricks::brick_pull("chembl")

chembl  <- biobricks::brick_load("chembl")$parquet
invisible(safely(fs::dir_delete)("cache/chembl"))
outputdir <- fs::dir_create("cache/chembl",recurse=T)
writeds  <- function(df,name){ arrow::write_dataset(df,fs::path(outputdir,name)) }

activity <- chembl$activities.parquet 

# Export Chemicals =============================================================
compound <- chembl$compound_structures.parquet |> 
  select(molregno, canonical_smiles, standard_inchi) |> collect() |> 
  group_by(molregno) |> mutate(cid = UUIDgenerate()) |> ungroup()

substances <- compound |> select(cid, molregno, canonical_smiles, standard_inchi) |> distinct() |>
  nest(data=-cid) |> mutate(data = map_chr(data,~ jsonlite::toJSON(as.list(.),auto_unbox=T)))

writeds(substances, "substances.parquet")

# Export Properties ============================================================
assay  <- chembl$assays.parquet |> collect() |> 
  group_by(assay_id) |> mutate(pid = UUIDgenerate()) |> ungroup() |>
  select(pid, assay_id,assay_desc=description)

properties = assay |> distinct() |>
  nest(data=-pid) |> mutate(data = map_chr(data,~ jsonlite::toJSON(as.list(.),auto_unbox=T)))

writeds(properties, "properties.parquet")

# Export Activities ============================================================
activity <- chembl$activities.parquet |> collect() |> 
  inner_join(assay,by="assay_id") |> inner_join(compound,by="molregno") 

train <- activity |> filter(standard_relation =="=") |> collect() |>
  select(cid,pid,qualifier=standard_relation,units=standard_units,value=standard_value)

activities <- activity |> 
  filter(!is.na(standard_value)) |>
  filter(nchar(canonical_smiles) < 200) |>
  filter(!grepl("[+-.]",canonical_smiles)) |>
  group_by(standard_type) |> 
  filter(n() > 1000) |> # only keep properties with 1000+ examples
  mutate(med_prop_val=median(standard_value)) |> 
  ungroup() |>
  mutate(stype = factor(standard_type, levels=unique(standard_type))) |>
  mutate(property_id = as.numeric(stype)) |>
  mutate(value = array(ifelse(standard_value>med_prop_val,1L,0L))) |>
  mutate(activity_id = row_number()) |>
  select(activity_id, stype, property_id, canonical_smiles, standard_value, med_prop_val, value)

writeds(activities, "activities.parquet")

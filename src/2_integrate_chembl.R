pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

# biobricks::brick_install("chembl")
# biobricks::brick_pull("chembl")

chembl  <- biobricks::brick_load("chembl")$parquet
invisible(safely(fs::dir_delete)("brick/chembl"))
outputdir <- fs::dir_create("brick/chembl", recurse = TRUE)
writeds <- function(df, name) {
  arrow::write_dataset(df, fs::path(outputdir, name))
}

# Export Chemicals =====================================================
compound <- chembl$compound_structures.parquet |>
  select(molregno, canonical_smiles, standard_inchi) |> collect() |>
  group_by(molregno) |> mutate(sid = UUIDgenerate()) |> ungroup()

substances <- compound |>
  select(sid, molregno, canonical_smiles, standard_inchi) |>
  distinct() |> nest(data = -sid) |> mutate(data =
  map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

writeds(substances, "substances.parquet")

# Export Properties ===================================================
assay  <- chembl$assays.parquet |> collect() |> 
  group_by(assay_id) |> mutate(pid = UUIDgenerate()) |> ungroup() |>
  select(pid, assay_id,assay_desc=description)

properties <- assay |> distinct() |>
  nest(data = -pid) |> mutate(data =
  map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

writeds(properties, "properties.parquet")

# Export Activities ============================================================
activity <- chembl$activities.parquet |> collect()
compound_smi <- chembl$compound_structures.parquet %>%
  select(molregno, canonical_smiles) %>%
  collect()
compound_smi <- compound_smi %>%
  column_to_rownames(var = "molregno")

spawn_progressbar <- function(x, .name = .pb, .times = 1) {
  .name <- substitute(.name)
  n <- nrow(x) * .times
  eval(substitute(.name <<- dplyr::progress_estimated(n)))
  x
}
get_smiles <- function(x, df, .pb) {
  .pb$tick()$print()
  df[x, ]
}
activity <- activity %>%
  group_by(molregno) %>%
  spawn_progressbar(.times = 0.18) %>%
  mutate(smiles = get_smiles(molregno, compound_smi, .pb)) %>%
  ungroup()

activity <- activity |>
  inner_join(assay, by = "assay_id") |>
  inner_join(compound, by = "molregno")

train <- activity |> filter(standard_relation == "=") |> collect() |>
  select(sid, pid, qualifier = standard_relation,
  units = standard_units, value = standard_value, smiles)

# activities <- activity |>
#   filter(!is.na(standard_value)) |>
#   filter(nchar(canonical_smiles) < 200) |>
#   filter(!grepl("[+-.]", canonical_smiles)) |>
#   group_by(standard_type) |>
#   filter(n() > 1000) |> # only keep properties with 1000+ examples
#   mutate(med_prop_val = median(standard_value)) |>
#   ungroup() |>
#   mutate(stype = factor(standard_type,
#     levels = unique(standard_type))) |>
#   mutate(property_id = as.numeric(stype)) |>
#   mutate(value = array(ifelse(standard_value > med_prop_val, 1L,
#     0L))) |>
#   mutate(activity_id = row_number()) |>
#   select(activity_id, stype, property_id, canonical_smiles,
#   standard_value, med_prop_val, value)

writeds(train, "activities.parquet")

pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

chembl  <- biobricks::bbload("chembl")
out <- fs::dir_create("staging/chembl", recurse = TRUE)
writeds <- \(df, name) { arrow::write_dataset(df, fs::path(out, name)) }
uuid <- uuid::UUIDgenerate

# Export Chemicals =====================================================
compound <- chembl$compound_structures |> head(100) |>
  select(molregno, canonical_smiles, standard_inchi) |> collect() |>
  group_by(molregno) |> mutate(sid = uuid()) |> ungroup()

substances <- compound |>
  select(sid, molregno, canonical_smiles, standard_inchi) |>
  distinct() |> nest(data = -sid) |> mutate(data =
  map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = TRUE)))

writeds(substances, "substances.parquet")

# Export Properties ===================================================

assay  <- chembl$assays |> collect() |> 
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

writeds(train, "activities.parquet")

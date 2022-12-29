# STORE `cache/tox21` with `substances.parquet`, `properties.parquet`, and `activities.parquet`
pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite)

# biobricks::brick_install("tox21")
# biobricks::brick_pull("tox21")

tox21  <- biobricks::brick_load("tox21")

tox21df <- tox21[[2]] |> collect()
for (i in seq(from = 4, to = 154, by = 2)) {
  df <- tox21[[i]] |> collect()
  tox21df <- rbind(tox21df, df)
}

invisible(safely(fs::dir_delete)("cache/tox21"))
outputdir <- fs::dir_create("cache/tox21", recurse = TRUE)
writeds <- function(df, name) {
  arrow::write_dataset(df, fs::path(outputdir, name))
}

# Export Chemicals ====================================================
tox21chems <- tox21df |> group_by(SAMPLE_ID) |>
  mutate(sid = UUIDgenerate()) |> ungroup()

substances <- tox21chems |> 
  select(sid, SAMPLE_NAME, SAMPLE_ID, CAS, PUBCHEM_CID, SMILES, TOX21_ID) |> 
  distinct() |> nest(data=-sid) |> mutate(data = 
  map_chr(data,~ jsonlite::toJSON(as.list(.),auto_unbox=T)))

writeds(substances, "substances.parquet")


# Export Properties ====================================================
pcols <- c("PROTOCOL_NAME", "SAMPLE_DATA_TYPE", "ASSAY_OUTCOME",
  "CHANNEL_OUTCOME", "AC50", "EFFICACY", "REPRODUCIBILITY",
  "CURVE_RANK", "FLAG")

tox21props <- tox21chems |> group_by(!!!syms(pcols)) |>
  mutate(pid = UUIDgenerate()) |> ungroup()

properties <- tox21props |> select(pid, !!!syms(pcols)) |> distinct() |>
  nest(data = -pid) |> mutate(data =
  map_chr(data, ~ jsonlite::toJSON(as.list(.), auto_unbox = T )))

writeds(properties, "properties.parquet")

# Export Activities ====================================================
activities <- tox21props |>
  mutate(source_id = row_number()) |>
  mutate(source_id = paste0("tox21-tox21.parquet", source_id)) |>
  select(source_id, sid, pid, qualifier = ASSAY_OUTCOME, value = AC50)

writeds(activities, "activities.parquet")

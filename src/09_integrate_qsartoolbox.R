pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, reticulate)

rdkit <- reticulate::import('rdkit')
qsartoolbox <- biobricks::bbassets("qsartoolbox")

tbls <- purrr::map(qsartoolbox, \(x){ arrow::open_dataset(x) |> head() |> collect() })

tblcols <- purrr::map_dfr(names(tbls), \(name){ 
    tibble(colname = colnames(tbls[[name]])) |> mutate(tbl = name) |> select(tbl,colname)
}) 

# CALCULATED VALUES
calculator <- arrow::open_dataset(qsartoolbox$calculator_parquet) |> collect() |> rename(calculator_id = id)
scalar_calculation <- arrow::open_dataset(qsartoolbox$scalar_calculation_parquet) |> collect()
smiles <- arrow::open_dataset(qsartoolbox$smiles_parquet) |> collect() |> rename(smiles_id = id)

smiles_to_inchi <- purrr::possibly(function(smiles){
    mol = rdkit$Chem$MolFromSmiles(smiles)
    rdkit$Chem$inchi$MolToInchi(mol)
},otherwise=NA_character_,)

calcdf <- scalar_calculation |> 
    inner_join(calculator, by = "calculator_id") |> 
    inner_join(smiles, by = "smiles_id")

calcdf$inchi <- calcdf$smiles |> purrr::map_chr(smiles_to_inchi)

# MEASURED VALUES
# TODO Make the below work

# smiles <- arrow::open_dataset(qsartoolbox$smiles_parquet) |> collect() |> rename(smiles_id = id)
# substance <- {
#     a <- arrow::open_dataset(qsartoolbox$substance_parquet) |> collect()
#     b <- a |> filter(substance_type=="monoc") # only consider substances with a single component
#     b |> select(smiles_id, substance_id=id) |> 
#         inner_join(smiles, by = "smiles_id") |>
#         filter(!is.na(substance_id)) |>
#         filter(smiles != "")
# }

# substance_measured_data <- {
#     a <- arrow::open_dataset(qsartoolbox$substance_measured_data_parquet) |> collect()
#     b <- a |> filter(same_test_material) |> select(substance_id, measured_data_id)
#     b |> inner_join(substance, by = "substance_id")
# }

# endpoint <- {
#     # database_funits <- arrow::open_dataset(qsartoolbox['datanase_path_funits']) |> collect()
#     # database_path <- arrow::open_dataset(qsartoolbox['database_path']) |> collect() |> rename(database_path_id = id)
#     # database_path <- database_path |> inner_join(database_funits, by = "database_path_id")

#     # databases <- arrow::open_dataset(qsartoolbox['databases']) |> collect() |> rename(database_id = id) |> select(-data_count)
#     # databases <- databases |> inner_join(database_path, by = "database_id")

#     endpoint_paths <- arrow::open_dataset(qsartoolbox$endpoint_paths_parquet) |> collect() |> rename(endpoint_path_id = id)
#     a <- arrow::open_dataset(qsartoolbox$endpoint_path_parquet) |> collect() |> select(endpoint_path_id = id, parent_id)
#     a |> inner_join(endpoint_paths, by = "endpoint_path_id")
# }

# raw_measured_data <- arrow::open_dataset(qsartoolbox$measured_data) |> collect() |> filter(same_test_material)
# measured_data <- raw_measured_data |> select(measured_data_id=id, endpoint_path_id, study, unit, meta_data, substance_id, mean_data)
# measured_data <- measured_data |> inner_join(endpoint, by = c("endpoint_path_id"))
# measured_data <- measured_data |> inner_join(substance_measured_data, by = c("substance_id", "measured_data_id"))

# measured_data$meta_data <- gsub("\'", "\"",gsub("\"", '', measured_data$meta_data))
# m2$meta_data <- map(measured_data$meta_data, ~ purrr::possibly(fromJSON,otherwise = list())(.x, simplifyVector = TRUE),.progress=TRUE)

# all_keys <- unlist(lapply(meta_data_list, names))
# key_counts <- table(all_keys) |> sort(decreasing=TRUE) |> as.data.frame() |>tibble()

# endpoint = map_chr(meta_data_list, ~ if('Endpoint' %in% names(.x)){.x[['Endpoint']]}else{NA_character_})
# guideline = map_chr(meta_data_list, ~ if('Guideline' %in% names(.x)){.x[['Guideline']]}else{NA_character_})


# df <- measured_data |> filter(endpoint_path_id == 32, !is.na(mean_data))
# df[1,] |> pivot_longer(everything(), values_transform = as.character)

# df2 <- raw_measured_data |> filter(endpoint_path_id == 32)
# df2 |> count(mean_data,sort=T)

# df[1,] |> pivot_longer(everything(), values_transform = as.character)
pacman::p_load(biobricks, tidyverse, arrow, uuid, jsonlite, reticulate)
bb <- reticulate::import("biobricks")

bdb <- bbload("bindingdb")

walk(seq_along(bdb), \(i){
    df <- bdb[[i]] |> head() |> collect()
    print(names(bdb)[[i]])
    scan(what="character", n=1)
    print(df)
    scan(what="character", n=1)
    print(names(bdb[[i]]))
    scan(what="character", n=1)
})

kidf <- bdb$ki_result |> collect() |> tibble()

raw <- kidf |> select(smiles=`Ligand SMILES`, inchi=`Ligand InChI`, 
    target=7,
    target_link=25,
    Ki_nM = 9, IC50_nM=10, Kd_nM=11, EC50_nM=12)

raw |> filter(!is.na(Ki_nM)) |> nrow() # 192190
raw |> filter(!is.na(Kd_nM)) |> nrow() # 101156
raw |> filter(!is.na(EC50_nM)) |> nrow() # 38849
raw |> filter(!is.na(IC50_nM)) |> nrow() # 179034

raw |> filter(is.na(Ki_nM), is.na(Kd_nM), is.na(EC50_nM), is.na(IC50_nM)) 

a <- bdb |> filter(`Ligand SMILES` == 'CC(C)C1(CCc2ccc(O)cc2)CC(=O)C(Sc2cc(C)c(NS(=O)(=O)c3ccc(cn3)C(F)(F)F)cc2C(C)(C)C)C(=O)O1')
a |> pivot_longer(everything()) |> filter(!is.na(value)) |> print(n=100)


tonum <- function(x){
    num = as.numeric(x)
    if(is.na(num)){
        num 
    }

}
raw |> 
    filter(!is.na(Ki_nM)) |>  # 192190 rows
    mutate(Ki_nM = gsub("[<>]","",Ki_nM)) |>
    mutate(num = as.numeric(Ki_nM)) |>
    filter(is.na(num)) # 129,936 rows

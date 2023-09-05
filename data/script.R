library(dplyr)
library(rvest)
library(data.table)
library(lubridate)

site <- read_html("https://www.tesourotransparente.gov.br/ckan/dataset/transferencias-constitucionais-para-municipios")

links <- site %>%
  html_nodes("a") %>%
  html_attr("href")

# Filtrar os links 
links <- grep("TransferenciaMensalMunicipios2018|TransferenciaMensalMunicipios2019|TransferenciaMensalMunicipios202.*\\.csv$", links, value = TRUE)
# Baixar e salvar os arquivos com seus nomes originais
for (link in links) {
  nome_arquivo <- basename(link)
  download.file(link, nome_arquivo, mode = "wb")
}


transferencias_tesouro <- list.files(pattern = "\\.csv$", full.names = TRUE) 

transferencias_tesouro <- rbindlist(lapply(transferencias_tesouro, function(file) {
  fread(file, encoding = "Latin-1")
}))


transferencias_tesouro <- transferencias_tesouro |> 
  filter(UF == 'SE') |> 
  relocate(.after = last_col(), contains("Decêndio")) |> 
  mutate(Mês = ymd(paste0(ANO, "-", Mês, "-01")),
         `Valor total` = `1º Decêndio`+`2º Decêndio`+`3º Decêndio`,
         Município = case_when(Município  == 'Amparo de São Francisco' ~ 'Amparo do São Francisco',
                               Município  == 'Gracho Cardoso' ~ 'Graccho Cardoso',
                               Município  == 'Itaporanga DAjuda' ~ "Itaporanga D'Ajuda",
                               TRUE ~ Município)) |> 
  select(-c(V10, UF, ANO)) |> 
  relocate(Mês, .before = everything())

arquivos_csv <- dir(pattern = ".csv")
file.remove(arquivos_csv)  

FPM_tesouro <- transferencias_tesouro |> 
  filter(Transferência == 'FPM') 

saveRDS(transferencias_tesouro, 'data/transferencias_tesouro.rds')
saveRDS(FPM_tesouro, 'data/FPM_tesouro.rds')

write.csv(transferencias_tesouro, "data/transferencias_tesouro.csv", row.names = FALSE)
write.csv(FPM_tesouro, "data/FPM_tesouro.csv", row.names = FALSE)

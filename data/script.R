library(dplyr)
library(tidyr)
library(rvest)
library(data.table)
library(lubridate)
library(httr)
library(doParallel)

site <- read_html("https://www.tesourotransparente.gov.br/ckan/dataset/transferencias-constitucionais-para-municipios")

links <- site %>%
  html_nodes("a") %>%
  html_attr("href")

# Filtrar os links 
links <- grep("TransferenciaMensalMunicipios2018|TransferenciaMensalMunicipios2019|TransferenciaMensalMunicipios202.*\\.csv$", links, value = TRUE)


# Função para baixar e processar um arquivo
download_and_process <- function(url) {
  arquivo <- sub(".*/", "", url)  # Obtém o nome do arquivo do URL
  
  # Verifica se o arquivo existe antes de fazer o download
  response <- tryCatch(
    {
      HEAD(url)
    },
    error = function(e) {
      return(NULL)
    }
  )
  
  # Se a resposta é NULL, o arquivo não existe, então retorna
  if (is.null(response)) {
    message(paste("Arquivo não encontrado:", arquivo))
    return(NULL)
  }
  
  tryCatch(
    {
      download.file(url, destfile = arquivo, mode = "wb")
      unzip(arquivo)
    },
    error = function(e) {
      message(paste("Erro ao baixar o arquivo:", arquivo))
      return(NULL)
    }
  )
}

# Vetor de URLs a serem processadas
urls <- links

# Número de processos paralelos a serem executados
num_cores <- parallel::detectCores()
registerDoParallel(cores = num_cores)

# Executa o download e processamento em paralelo
results <- foreach(url = urls, .packages = c("httr", "lubridate"), .combine = c) %dopar% {
  download_and_process(url)
}



transferencias_tesouro <- list.files(pattern = "\\.csv$", full.names = TRUE) 

transferencias_tesouro <- rbindlist(lapply(transferencias_tesouro, function(file) {
  fread(file, encoding = "Latin-1")
}), fill = TRUE)
#fill = TRUE, para quando há divergência no número de colunas


arquivos_csv <- dir(pattern = ".csv")
file.remove(arquivos_csv)  


transferencias_tesouro <- transferencias_tesouro |> 
  select(-starts_with("V")) |> 
  filter(UF == 'SE') |> 
  mutate_at(vars(c("1º Decêndio", "2º Decêndio", "3º Decêndio")), as.numeric) |> 
  unite(ANO, ANO, Ano, sep = "", na.rm = TRUE) |> 
  relocate(.after = last_col(), contains("Decêndio")) |> 
  mutate(Mês = ymd(paste0(ANO, "-", Mês, "-01")),
         `Valor mensal` = `1º Decêndio`+`2º Decêndio`+`3º Decêndio`,
         Município = case_when(Município  == 'Amparo de São Francisco' ~ 'Amparo do São Francisco',
                               Município  == 'Gracho Cardoso' ~ 'Graccho Cardoso',
                               Município  == 'Itaporanga DAjuda' ~ "Itaporanga D'Ajuda",
                               TRUE ~ Município)) |> 
  select(-c("UF", "ANO")) |> 
  relocate(Mês, .before = everything())


### IPCA ###
ipca <- readRDS(url("https://github.com/Rodslater/inflacao/raw/main/data/ipca.rds"))

############


transferencias_tesouro <- transferencias_tesouro |> 
  left_join(ipca, by = 'Mês' ) |> 
  fill(indice_ipca, .direction = 'down') |> 
  mutate(inflacao = tail(indice_ipca, n=1)/indice_ipca,
         `Valor mensal real IPCA` = round(`Valor mensal`*inflacao,2)) |> 
  select(-c(inflacao, indice_ipca))


FPM_tesouro <- transferencias_tesouro |> 
  filter(Transferência == 'FPM') 


saveRDS(transferencias_tesouro, 'data/transferencias_tesouro.rds')
saveRDS(FPM_tesouro, 'data/FPM_tesouro.rds')

write.csv(transferencias_tesouro, "data/transferencias_tesouro.csv", row.names = FALSE)
write.csv(FPM_tesouro, "data/FPM_tesouro.csv", row.names = FALSE)

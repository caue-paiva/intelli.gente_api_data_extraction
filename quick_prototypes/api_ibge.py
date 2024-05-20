import requests

{
   "id_muni":1829,
   "nome_muni": "ajhga",
   "ano": 2010,
   "nome_dado": "ahugs",
   "tipo_dado" :float,
   "value": 1729862.7
}

def process_ibge_agregate_api(api_return:list[dict], year:int)->list[dict]:
   processed_dict_list:list[dict] = []
   
   for data_point in api_return:
      data_name:str = data_point["variavel"]
      data_unit: str = data_point["unidade"]
      data_type:object = str
      multiply_amount:int 

      if data_unit == "Mil Reais":
         data_type = float
         multiply_amount = 1000

      lista_dados: list[dict] = data_point["resultados"][0]["series"]
      for dado in lista_dados:
         id_muni = dado["localidade"]["id"]
         nome = dado["localidade"]["nome"]

         serie_histo:dict = dado["serie"]

         for ano, valor in serie_histo.items():
            valor_dado = valor #mudar isso para ser mais flexível
            processed_dict_list.append({
               "id_muni": id_muni,
               "nome_muni": nome,
               "ano": ano,
               "nome_dado": data_name,
               "tipo_dado" : data_type,
               "value": valor_dado
            })

   return processed_dict_list

base_url = "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos/{periodos}/variaveis/{variaveis}"

agregado:int = 9606
periodos:int = -2

variaveis:list[int] = [93]
id_municipios:list[int] = [3550308]

variaveis_str:str = '|'.join(map(str, variaveis))

params = {
    'localidades': f'N6{id_municipios}'
}

print(str(id_municipios))
url = base_url.format(agregado=agregado, periodos=-7, variaveis=variaveis_str)
response = requests.get(url, params=params, verify=False)

# Print the response (or handle it as needed)
data = response.json()
print(data)


processed_data_list:list[dict] = process_ibge_agregate_api(data,2010)

for elem in processed_data_list:
    print(elem)
    print("\n\n\n\n")







[ #o elemento mais externo é uma lista, onde cada elemento é uma variável (dado bruto)
  {
     "id": 123, #id do dado bruto dentro daquele agregado
     "variavel": "akasa", #nome do dado bruto 
     "unidade": "Mil reais", #como o dado esta representado
     "resultados": [ #onde estão os dados em si
        { #primeiro índice é um dict
           "series": [ #cada elemento dessa lista é um dict que contem o dado para cada município da request
              {
                 "localidade" : {
                     "id": 1827827, #id do município
                     "nome": "nome cidade"
                  },
                  "serie": { #aqui que vão estar os dados
                     "ano": "dado em string" 
                  }
              } #caso tenha mais de um município, teria mais um dict similar na lista
           ]
        }
     ]
   }
]




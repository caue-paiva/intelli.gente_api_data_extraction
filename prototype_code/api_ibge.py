import requests ,json

{
   "id_muni":1829,
   "nome_muni": "ajhga",
   "ano": 2010,
   "nome_dado": "ahugs",
   "tipo_dado" :float,
   "value": 1729862.7
}

def process_single_api_result(list_of_cities:list[dict],data_name:str,data_type:object)->list[dict]:
   processed_dict_list:list[dict] = []

   for city_dict in list_of_cities:
      city_id:str = city_dict["localidade"]["id"]
      city_name:str = city_dict["localidade"]["nome"]
      serie_histo:dict = city_dict["serie"]

      for ano, valor in serie_histo.items():
         valor_dado = valor #mudar isso para ser mais flexível
         processed_dict_list.append({
                     "id_muni": city_id,
                     "nome_muni": city_name,
                     "ano": ano,
                     "nome_dado": data_name,
                     "tipo_dado" : data_type,
                     "valor": valor_dado
         })

   return processed_dict_list

def process_ibge_agregate_api(api_return:list[dict])->list[dict]:
   processed_dict_list:list[dict] = []
   
   for variable in api_return:
      data_name:str = variable["variavel"]
      data_unit: str = variable["unidade"]
      data_type:object = str
      multiply_amount:int 

      if data_unit == "Mil Reais":
         data_type = float
         multiply_amount = 1000


      results_list: list[dict] = variable["resultados"]
     
      for result in results_list:
         list_of_cities:list[dict] = result["series"]
         processed_result:list[dict] = process_single_api_result(list_of_cities,data_name,data_type)

         if not processed_dict_list:
            processed_dict_list = processed_result
         else:
            add_list_values = lambda x,y: float(x["valor"]) + float(y["valor"])
            summed_vals = list(map(add_list_values,processed_dict_list,processed_result))
            
            for index, val in enumerate(summed_vals):
               processed_dict_list[index]["valor"] = val

         
   return processed_dict_list

base_url = "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos/{periodos}/variaveis/{variaveis}"
url2 = "https://servicodados.ibge.gov.br/api/v3/agregados/2409/metadados"

agregado:int = 2409
periodos:int = -2

variaveis:list[int] = [96]
id_municipios:list[int] = [1100072,1100023]
categorias = [0,104563,104562]
variaveis_str:str = '|'.join(map(str, variaveis))

params = {
   # "classificacao" : "12235[0,104563,104562]",
    'localidades': f'N6{id_municipios}'
}

print(str(id_municipios))
url = base_url.format(agregado=agregado, periodos=-7, variaveis=96)
url3 =  "https://servicodados.ibge.gov.br/api/v3/agregados/2409/periodos/-2/variaveis/96/?classificacao=12235[104562,104563]|1[1]"
url4 =  "https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/-2/variaveis/517|6575"

response = requests.get(url4, params=params, verify=False)
# Print the response (or handle it as needed)
print(response.status_code)
data = response.json()
#print(data)

with open("teste3.json", "w") as f:
   json.dump(data,f, indent=4, ensure_ascii=False)


processed_data_list:list[dict] = process_ibge_agregate_api(data)
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




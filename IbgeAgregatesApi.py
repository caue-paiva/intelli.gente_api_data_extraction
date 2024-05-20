from AbstractApiInterface import AbstractApiInterface
from DataPoint  import DataPoint, DataPointTypes
import requests,json , os
import pandas as pd



"""
TODO

1) Fazer com que o extrador de dados da API para os data points consiga extrair dados que vem com diversas categorias, ver como isso pode funcionar e como 
não fazer isso quebrar a lógica antiga. Acho que isso deve finalizar a lógica do extrator do IBGE agregados

"""


class IbgeAgregatesApi(AbstractApiInterface):

   api_name:str
   goverment_agency:str
   _data_map: dict[str, dict]

   def __init__(self, api_name: str, goverment_agency: str, api_referen_json_path:str) -> None:
      self.api_name = api_name
      self.goverment_agency = goverment_agency

      try:
         with open(os.path.join(api_referen_json_path), "r") as f:
            loaded_data = json.load(f)
            if not isinstance(loaded_data,dict):
               raise IOError("objeto json não está na forma de um dicionário do python")
            self._data_map: dict[str, dict] = loaded_data
      
      except Exception as e:
           raise RuntimeError("Não foi possível carregar o JSON que mapea os dados do DB para a API")
      
   def _db_to_api_data_map(self, db_data_list: list[str|int] = []) -> tuple[dict[int,list] , dict[int,dict]]:
      """
      Tuple: (
         var dict : {
            2198298: [189289,2108291]
         },
         classification_dict: {
            2198298: {
               189289: "18278"
            }
         }
      )
      """

      variables_dict: dict[int, list[int]] = {} #dicionario cuja key é o agregado e o value é uma lista de variáveis que pertence a esse agregado
      classification_dict: dict[int, dict[int,list]] = {}

      for key,val in self._data_map.items():
         if key in db_data_list or not db_data_list: #se o dado estiver na lista passada ou se lista for vazia, nesse ultimo caso coloca todos os dados do mapping
            var:int | None = val.get("variavel") #a variável pode não aparecer em alguns dados buscados
            aggregate: int = val["agregado"] 
            classification:str | None = val.get("classificacao")

            if var is None and classification is None:
               raise IOError("Ambos a variável e a classificação não existem ou não None")

            if classification is not None: #caso especial com classificação
               classification_dict.setdefault(aggregate,{})
               var_key:int = var if var is not None else -1
               classification_dict[aggregate][var_key] = classification
            else:  #caso normal so com a variável
               variables_dict.setdefault(aggregate,[])
               variables_dict[aggregate].append(var)
            

      return variables_dict,classification_dict
   
   def __find_data_name_by_id(self,variable_id:int)->str | None:
      for key, value in self._data_map.items():
        if value.get("variavel") == variable_id:
            return key
      return None


   def __process_single_api_result(self,list_of_cities:list[dict],data_name:str,data_unit:str)->list[DataPoint] :
      return_data_points:list[DataPoint] = []

      for city in list_of_cities: #loop pelos municípios
         city_id:int | None = int(city.get("localidade", {}).get("id"))
         city_name: str | None = city.get("localidade", {}).get("nome")
         time_series: dict | None = city.get("serie") #dicionário com a série histórica dos dados

         if any(x is None for x in [city_id,city_name,time_series]):  #verifica se foi possível acessar todos os campos
                  raise IOError(f"Não foi possível obter uma variável da lista {[city_id,city_name,time_series]}")

         for year, value in time_series.items(): #loop pelo dicionario com o ano como chave e o valor do dado como value
            
            new_data_point:DataPoint = DataPoint(city_id, year, data_name, DataPointTypes.FLOAT,value) #cria um novo ponto de dado, mas sem o numero de multiplicação #nem o valor   
            inference_result:bool = new_data_point.infer_dtype_and_multiply_amnt(data_unit) #tentar inferir o numero pra multiplicar o valor da unidade obtida anteriormente
            if not inference_result:
               print("Não foi possível inferir o tipo de dado e qntd de multiplicar do dado")
                  
            new_data_point.value = new_data_point.multiply_value() #campo de valor do data_point recebe o valor lida da API e multiplicado
            return_data_points.append(new_data_point) #adiciona esse data point na lista

      return return_data_points

   def __api_to_data_points(self,api_response:list[dict])->list[DataPoint]:
      return_data_points:list[DataPoint] = []
      
      for variable in api_response: #loop por cada dado bruto/variável na resposta
         variable_id: int | None = int(variable .get("id")) #id da variável
         if variable_id is None:
            raise IOError("não foi possível acessar o id da variável")
         
         data_name:str | None  = self.__find_data_name_by_id(variable_id)
         data_unit: str | None = variable .get("unidade")  #unidade que o dado está
         
         results_list:list[dict]| None = variable .get("resultados", [])
         
         if any(x is None for x in [variable_id,data_unit,results_list,data_name]): #verifica se foi possível acessar todos os campos
            raise IOError(f"Não foi possível obter uma variável da lista { [variable_id,data_unit,results_list,data_name]}")

         for result in results_list:
            list_of_cities: list[dict]| None = result["series"] #valores do resultado
            processed_result:list[DataPoint] = self.__process_single_api_result(list_of_cities,data_name,data_unit)

            if not return_data_points:
               return_data_points = processed_result
            else:
               add_data_point_values = lambda x, y: x.multiply_value() + y.multiply_value()
               summed_vals = list(map(add_data_point_values, return_data_points, processed_result))
            
               for index, val in enumerate(summed_vals):
                  return_data_points[index].value = val
                  
      return return_data_points
   
   def __make_api_call(self,time_series_len:int,cities:list[int],aggregate:int ,variables:str = "", classification:str = "")->list[dict]:
      params:dict = {}
      if classification:
         params = {'localidades': f'N6{cities}'}
         if "[" in variables and "]" in variables:
            raise IOError("Não é possível realizar uma chamada da API com uma classificação e mais de uma variável")
      else:
         params = {"classificacao": classification ,'localidades': f'N6{cities}' }

      base_url:str = "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos/{periodos}/variaveis/{variaveis}"

      url:str = base_url.format(agregado=aggregate , periodos= (-time_series_len), variaveis=variables)
      response = requests.get(url, params=params, verify=False)

      if response.status_code == 200: #request teve sucesso
         response_data:list[dict] = response.json()
         return self.__api_to_data_points(response_data) #adiciona os elementos retornados a lista final de pontos de dados
      else:
         raise RuntimeError("Falha na Request para a API")

   def extract_data_points(self, cities: list[int], db_data_list:list[str|int] = [] , time_series_len: int = 0) -> list[DataPoint]:
      if time_series_len > self.MAX_TIME_SERIES_LEN:
         raise IOError(f"tamanho da série temporal em anos excede o limite de {self.MAX_TIME_SERIES_LEN} anos")
      
      if time_series_len == 0:
         time_series_len = self.MAX_TIME_SERIES_LEN

      if not cities:
         #lógica para incluir todas as cidades
         pass

      variables_api_calls, classification_api_calls = self._db_to_api_data_map(db_data_list)
      api_data_points: list[DataPoint] = []
   
      for aggregate in variables_api_calls: #faz as chamadas de APIs que somente tem variáveis
         str_data_variables:str = '|'.join(map(str, variables_api_calls[aggregate])) #transforma as variáveis da API em str 
         api_data_points.extend(self.__make_api_call(time_series_len,cities,aggregate,str_data_variables))
         print("add a seguinte coisa de variável ", api_data_points[-1])
      
      for aggregate in classification_api_calls: #faz as chamadas de APIs que somente tem variáveis
         for var_key, classification in classification_api_calls[aggregate].items():
            api_data_points.extend(self.__make_api_call(time_series_len,cities,aggregate,str(var_key),classification)) #adiciona os elementos retornados a lista final de pontos de dados
      
      return api_data_points

if __name__ == "__main__":
   api1 = IbgeAgregatesApi("api agregados", "ibge","IbgeAgregatesApiDataMap.json")

   d_points:list[DataPoint] = api1.extract_data_points([1100072,1100023],time_series_len=7)
   #print(d_points[0].data_type)
   df : pd.DataFrame = api1.data_points_to_df(d_points)
   print(df.head(5))
   print(df.shape)
   print(df.info())

   df.to_csv(os.path.join("dados_extraidos","base_agregados_ibge.csv"))
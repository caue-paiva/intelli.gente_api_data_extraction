from AbstractApiInterface import AbstractApiInterface
from DataPoint  import DataPoint, DataPointTypes
import requests , json , os
import pandas as pd
from typing import Callable




"""
TODO

1) criar uma lógica nessa classe (e talvez na superclasse de forma mais geneŕica) para lidar com o ID dos agregados na api de agregados do IBGE

"""


class IbgeAgregatesApi(AbstractApiInterface):

   api_name:str
   goverment_agency:str
   _data_map: dict[str, dict]
   _calculated_data_functions: dict[str, dict] | None

   def __init__(self, api_name: str, goverment_agency: str, api_referen_json_path:str) -> None:
      self.api_name = api_name
      self.goverment_agency = goverment_agency

      try:
         with open(os.path.join(api_referen_json_path), "r") as f:
            loaded_data = json.load(f)
            if not isinstance(loaded_data,dict):
               raise IOError("objeto json não está na forma de um dicionário do python")
            self._data_map: dict[str, dict] = loaded_data["dados_diretos"]
            self._calculated_data_functions = loaded_data.get("dado_calculados")
      
      except Exception as e:
           raise RuntimeError("Não foi possível carregar o JSON que mapea os dados do DB para a API")
      
   def _db_to_api_data_map(self, db_data_list: list[str|int] = []) -> dict[int, list[int]]:
    
      return_dict: dict[int, list[int]] = {} #dicionario cuja key é o agregado e o value é uma lista de variáveis que pertence a esse agregado
      for key,val in self._data_map.items():
         if key in db_data_list or not db_data_list: #se o dado estiver na lista passada ou se lista for vazia, nesse ultimo caso coloca todos os dados do mapping
            var:int = val["variavel"] 
            aggregate: int = val["agregado"] 

            if aggregate not in return_dict: #chave do agregado n existe no dict
               return_dict[aggregate] = [var] #coloca essa chave com um lista com a variavel
            else:
               return_dict[aggregate].append(var) #se já existir é so dar append na variável
      
      return return_dict
   
 
   def __find_data_name_by_id(self,variable_id:int)->str | None:
      for key, value in self._data_map.items():
        if value.get("variavel") == variable_id:
            return key
      return None

   def __api_to_data_points(self,api_response:list[dict])->list[DataPoint]:
      return_data_points:list[DataPoint] = []
      
      for data_point in api_response: #loop por cada dado bruto/variável na resposta
         variable_id: int | None = int(data_point.get("id")) #id da variável
         if variable_id is None:
            raise IOError("não foi possível acessar o id da variável")
         
         data_name:str | None  = self.__find_data_name_by_id(variable_id)
         data_unit: str | None = data_point.get("unidade")  #unidade que o dado está
         results: list[dict]| None = data_point.get("resultados", [])[0].get("series", []) #valores do resultado

         if any(x is None for x in [variable_id,data_unit,results,data_name]): #verifica se foi possível acessar todos os campos
               raise IOError(f"Não foi possível obter uma variável da lista { [variable_id,data_unit,results,data_name]}")
         
         for city in results: #loop pelos municípios
            city_id:int | None = int(city.get("localidade", {}).get("id"))
            city_name: str | None = city.get("localidade", {}).get("nome")
            time_series: dict | None = city.get("serie") #dicionário com a série histórica dos dados

            if any(x is None for x in [city_id,city_name,time_series]):  #verifica se foi possível acessar todos os campos
               raise IOError(f"Não foi possível obter uma variável da lista {[city_id,city_name,time_series]}")

            for year, value in time_series.items(): #loop pelo dicionario com o ano como chave e o valor do dado como value
               new_data_point:DataPoint = DataPoint(city_id, year, data_name, DataPointTypes.FLOAT) #cria um novo ponto de dado, mas sem o numero de multiplicação
               #nem o valor
               
               inference_result:bool = new_data_point.infer_dtype_and_multiply_amnt(data_unit) #tentar inferir o numero pra multiplicar o valor da unidade obtida anteriormente
               if not inference_result:
                  print("Não foi possível inferir o tipo de dado e qntd de multiplicar do dado")
               
               new_data_point.value = new_data_point.multiply_value(value) #campo de valor do data_point recebe o valor lida da API e multiplicado
               return_data_points.append(new_data_point) #adiciona esse data point na lista
                  
      return return_data_points
   
   def extract_data_points(self, cities: list[int], db_data_list:list[str|int] = [] , time_series_len: int = 0) -> list[DataPoint]:
      if time_series_len > self.MAX_TIME_SERIES_LEN:
         raise IOError(f"tamanho da série temporal em anos excede o limite de {self.MAX_TIME_SERIES_LEN} anos")
      
      if time_series_len == 0:
         time_series_len = self.MAX_TIME_SERIES_LEN

      if not cities:
         #lógica para incluir todas as cidades
         pass


      api_data_variables: dict[int,list[int]] = self._db_to_api_data_map(db_data_list)
      base_url = "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}/periodos/{periodos}/variaveis/{variaveis}"

      api_data_points: list[DataPoint] = []
      params = {'localidades': f'N6{cities}'}

      for aggregate in api_data_variables:
         str_data_variables:str = '|'.join(map(str, api_data_variables[aggregate])) #transforma as variáveis da API em str 
         
         url:str = base_url.format(agregado=aggregate , periodos= (-time_series_len), variaveis=str_data_variables)
         response = requests.get(url, params=params, verify=False)

         if response.status_code == 200: #request teve sucesso
            response_data:list[dict] = response.json()
            api_data_points.extend(self.__api_to_data_points(response_data)) #adiciona os elementos retornados a lista final de pontos de dados
         else:
            raise RuntimeError("Falha na Request para a API")
         
      return api_data_points



if __name__ == "__main__":
   api1 = IbgeAgregatesApi("api agregados", "ibge","IbgeAgregatesApiDataMap.json")

   d_points:list[DataPoint] = api1.extract_data_points([1100072,1100023],time_series_len=7)
   print(d_points[0].data_type)
   df : pd.DataFrame = api1.data_points_to_df(d_points)
   print(df.head(5))
   print(df.shape)
   print(df.info())

"""

  def __create_calculated_data_funcs(self, dict_template:dict)->None:
      functions_dict: dict[str, Callable[[pd.Series], pd.Series]] = {}

      for key, val in  dict_template:
         operand1: str = val["operando1"]
         operand2: str = val["operando2"]
         operation: str = val["operacao"]

         generated_func: Callable[[pd.Series], pd.Series] #a função vai receber um Dataframe e retornar 

         match (operation):
            case "+":
               generated_func = lambda x : x[ x[self.DB_DATA_IDENTIFIER_COLUMN == operand1] ] + x[x[ self.DB_DATA_IDENTIFIER_COLUMN == operand2]]
            case "-":
               generated_func = lambda x : x[ x[self.DB_DATA_IDENTIFIER_COLUMN == operand1] ] - x[x[ self.DB_DATA_IDENTIFIER_COLUMN == operand2]]
            case "*":
               generated_func = lambda x : x[ x[self.DB_DATA_IDENTIFIER_COLUMN == operand1] ] * x[x[ self.DB_DATA_IDENTIFIER_COLUMN == operand2]]
            case "/":
               generated_func = lambda x : x[ x[self.DB_DATA_IDENTIFIER_COLUMN == operand1] ] / x[x[ self.DB_DATA_IDENTIFIER_COLUMN == operand2]]
         
         functions_dict[key] = generated_func

      self.calculated_data_functions = functions_dict   

"""
